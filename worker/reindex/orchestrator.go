// Package reindex provides a fenced, resumable-by-operator index migration.
package reindex

import (
	"context"
	"distributed-cdc/worker/elastic"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"
)

var indexName = regexp.MustCompile(`^events-v[a-z0-9][a-z0-9_-]*$`)

const lockPath = "/cdc-reindex-locks/_doc/events"

type Orchestrator struct {
	Client *elastic.Client
	Poll   time.Duration
}

func (o *Orchestrator) alias(ctx context.Context, name string) (string, error) {
	var indices map[string]json.RawMessage
	if err := o.Client.Do(ctx, "GET", "/_alias/"+name, nil, &indices); err != nil {
		return "", err
	}
	if len(indices) != 1 {
		return "", fmt.Errorf("alias %s must have exactly one index", name)
	}
	for k := range indices {
		if !indexName.MatchString(k) {
			return "", fmt.Errorf("unexpected index %q", k)
		}
		return k, nil
	}
	return "", fmt.Errorf("missing alias %s", name)
}
func (o *Orchestrator) checkpoint(ctx context.Context, phase string, extra map[string]any) error {
	if extra == nil {
		extra = map[string]any{}
	}
	extra["phase"] = phase
	slog.Info("reindex checkpoint", "phase", phase, "details", extra)
	return o.Client.Do(ctx, "POST", "/cdc-reindex-locks/_update/events", map[string]any{"doc": extra}, nil)
}
func readAlias(index string) map[string]any {
	return map[string]any{"index": index, "alias": "events-read", "filter": map[string]any{"term": map[string]any{"deleted": false}}}
}
func (o *Orchestrator) count(ctx context.Context, index string) (int64, error) {
	var r struct {
		Count  int64 `json:"count"`
		Shards struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
	}
	err := o.Client.Do(ctx, "GET", "/"+index+"/_count", nil, &r)
	if err == nil && r.Shards.Failed != 0 {
		err = fmt.Errorf("count had failed shards")
	}
	return r.Count, err
}
func (o *Orchestrator) refresh(ctx context.Context, index string) error {
	var r struct {
		Shards struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
	}
	if err := o.Client.Do(ctx, "POST", "/"+index+"/_refresh", nil, &r); err != nil {
		return err
	}
	if r.Shards.Failed != 0 {
		return fmt.Errorf("refresh had failed shards")
	}
	return nil
}

// Run keeps a durable lock and the source write fence on EVERY failure. This is
// intentional: a timed-out task or alias request can still finish on the server.
// See docs/reindex.md before clearing a failed job. Never automatically roll back.
func (o *Orchestrator) Run(ctx context.Context, target string, definition json.RawMessage) error {
	if !indexName.MatchString(target) {
		return fmt.Errorf("target must match %s", indexName)
	}
	var def map[string]json.RawMessage
	if err := json.Unmarshal(definition, &def); err != nil {
		return err
	}
	if def == nil {
		return fmt.Errorf("index definition required")
	}
	for k := range def {
		if k != "settings" && k != "mappings" {
			return fmt.Errorf("unsupported index definition field %s", k)
		}
	}
	// Prevent a mapping migration from silently discarding the envelope or
	// transforming documents while still passing cardinality validation.
	var mapping struct {
		Source     json.RawMessage `json:"_source"`
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(def["mappings"], &mapping); err != nil {
		return err
	}
	if len(mapping.Source) != 0 || mapping.Properties["deleted"].Type != "boolean" || mapping.Properties["data"].Type != "object" {
		return fmt.Errorf("mapping must retain default _source, deleted:boolean and data:object")
	}
	var settings map[string]json.RawMessage
	if raw, ok := def["settings"]; ok {
		if err := json.Unmarshal(raw, &settings); err != nil {
			return err
		}
	}
	for k := range settings {
		switch k {
		case "number_of_shards", "number_of_replicas", "analysis", "refresh_interval":
		default:
			return fmt.Errorf("unsupported target setting %s", k)
		}
	}
	// The lock index is created during bootstrap. require_alias does not apply here.
	lock := map[string]any{"target": target, "phase": "locked", "started_at": time.Now().UTC()}
	if err := o.Client.Do(ctx, "PUT", "/cdc-reindex-locks/_create/events?refresh=wait_for", lock, nil); err != nil {
		return fmt.Errorf("acquire reindex lock: %w", err)
	}
	old, err := o.alias(ctx, "events-write")
	if err != nil {
		return err
	}
	read, err := o.alias(ctx, "events-read")
	if err != nil {
		return err
	}
	if read != old || target == old {
		return fmt.Errorf("aliases differ or target is current index")
	}
	if err = o.checkpoint(ctx, "creating", map[string]any{"source": old}); err != nil {
		return err
	}
	// PUT fails if target exists; never overwrite an index from another attempt.
	if err = o.Client.Do(ctx, "PUT", "/"+target, definition, nil); err != nil {
		return err
	}
	var block struct {
		Acknowledged       bool `json:"acknowledged"`
		ShardsAcknowledged bool `json:"shards_acknowledged"`
	}
	// Dedicated block API fences writes and waits for in-flight writes to finish.
	if err = o.Client.Do(ctx, "PUT", "/"+old+"/_block/write", nil, &block); err != nil {
		return err
	}
	if !block.Acknowledged || !block.ShardsAcknowledged {
		return fmt.Errorf("source write fence not acknowledged")
	}
	if err = o.refresh(ctx, old); err != nil {
		return err
	}
	expected, err := o.count(ctx, old)
	if err != nil {
		return err
	}
	if err = o.checkpoint(ctx, "fenced", map[string]any{"expected_count": expected}); err != nil {
		return err
	}
	var task struct {
		Task string `json:"task"`
	}
	body := map[string]any{"source": map[string]any{"index": old}, "dest": map[string]any{"index": target, "version_type": "external"}}
	if err = o.Client.Do(ctx, "POST", "/_reindex?wait_for_completion=false", body, &task); err != nil {
		return err
	}
	if task.Task == "" {
		return fmt.Errorf("reindex returned no task ID")
	}
	if err = o.checkpoint(ctx, "copying", map[string]any{"task": task.Task}); err != nil {
		return err
	}
	if err = o.waitTask(ctx, task.Task, expected); err != nil {
		return err
	}
	if err = o.refresh(ctx, target); err != nil {
		return err
	}
	actual, err := o.count(ctx, target)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("count mismatch: source=%d target=%d", expected, actual)
	}
	// Re-check names before publishing. All alias writers must honor the same lock.
	current, err := o.alias(ctx, "events-write")
	if err != nil {
		return err
	}
	currentRead, err := o.alias(ctx, "events-read")
	if err != nil {
		return err
	}
	if current != old || currentRead != old {
		return fmt.Errorf("aliases changed during reindex")
	}
	if err = o.checkpoint(ctx, "validated", nil); err != nil {
		return err
	}
	actions := []any{
		map[string]any{"remove": map[string]any{"index": old, "alias": "events-read", "must_exist": true}},
		map[string]any{"remove": map[string]any{"index": old, "alias": "events-write", "must_exist": true}},
		map[string]any{"add": readAlias(target)},
		map[string]any{"add": map[string]any{"index": target, "alias": "events-write", "is_write_index": true}},
	}
	var swapped struct {
		Acknowledged bool `json:"acknowledged"`
		Errors       bool `json:"errors"`
	}
	if err = o.Client.Do(ctx, "POST", "/_aliases", map[string]any{"actions": actions}, &swapped); err != nil {
		return err
	}
	if !swapped.Acknowledged || swapped.Errors {
		return fmt.Errorf("alias swap not fully acknowledged; inspect aliases before recovery")
	}
	if err = o.checkpoint(ctx, "promoted", nil); err != nil {
		return err
	}
	// Old index stays blocked as a diagnostic snapshot, not a current rollback copy.
	return o.Client.Do(ctx, "DELETE", lockPath+"?refresh=wait_for", nil, nil)
}
func (o *Orchestrator) waitTask(ctx context.Context, id string, expected int64) error {
	poll := o.Poll
	if poll <= 0 {
		poll = time.Second
	}
	for {
		var task struct {
			Completed bool            `json:"completed"`
			Error     json.RawMessage `json:"error"`
			Response  *struct {
				Total     int64             `json:"total"`
				Created   int64             `json:"created"`
				Updated   int64             `json:"updated"`
				Conflicts int64             `json:"version_conflicts"`
				TimedOut  bool              `json:"timed_out"`
				Failures  []json.RawMessage `json:"failures"`
			} `json:"response"`
		}
		if err := o.Client.Do(ctx, "GET", "/_tasks/"+id, nil, &task); err != nil {
			return err
		}
		if len(task.Error) > 0 && string(task.Error) != "null" {
			return fmt.Errorf("reindex task error: %s", task.Error)
		}
		if task.Completed {
			r := task.Response
			if r == nil || r.TimedOut || len(r.Failures) > 0 || r.Conflicts != 0 || r.Updated != 0 || r.Total != expected || r.Created != expected {
				return fmt.Errorf("reindex failed validation: %+v", r)
			}
			return nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
