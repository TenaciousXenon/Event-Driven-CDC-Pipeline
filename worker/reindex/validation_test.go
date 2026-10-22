package reindex

import (
	"context"
	"distributed-cdc/worker/elastic"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWaitTaskRejectsIncompleteOrFailedCopy(t *testing.T) {
	for _, raw := range []string{
		`{"completed":true}`,
		`{"completed":true,"error":{"type":"task_cancelled_exception"}}`,
		`{"completed":true,"response":{"total":2,"created":1}}`,
		`{"completed":true,"response":{"total":2,"created":2,"timed_out":true}}`,
		`{"completed":true,"response":{"total":2,"created":2,"updated":1}}`,
		`{"completed":true,"response":{"total":2,"created":2,"version_conflicts":1}}`,
		`{"completed":true,"response":{"total":3,"created":3}}`,
		`{"completed":true,"response":{"total":2,"created":2,"failures":[{}]}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, raw) }))
			defer s.Close()
			o := Orchestrator{Client: elastic.New(s.URL)}
			if err := o.waitTask(context.Background(), "node:7", 2); err == nil {
				t.Fatal("unsafe copy accepted")
			}
		})
	}
}
func TestWaitTaskPollsUntilComplete(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			fmt.Fprint(w, `{"completed":false}`)
		} else {
			fmt.Fprint(w, `{"completed":true,"response":{"total":0,"created":0}}`)
		}
	}))
	defer s.Close()
	o := Orchestrator{Client: elastic.New(s.URL), Poll: time.Millisecond}
	if err := o.waitTask(context.Background(), "node:1", 0); err != nil || calls != 2 {
		t.Fatal(err, calls)
	}
}
func TestWaitTaskCanBeCancelledWithoutUnlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("cancellation must not mutate remote state")
		}
		fmt.Fprint(w, `{"completed":false}`)
		cancel()
	}))
	defer s.Close()
	o := Orchestrator{Client: elastic.New(s.URL), Poll: time.Hour}
	if err := o.waitTask(ctx, "node:1", 2); err == nil {
		t.Fatal("cancelled job succeeded")
	}
}
func TestUnsafeDefinitionsRejectedBeforeLock(t *testing.T) {
	for _, raw := range []string{
		`null`, `{}`, `{"aliases":{}}`,
		`{"mappings":{"_source":{"enabled":false},"properties":{"deleted":{"type":"boolean"},"data":{"type":"object"}}}}`,
		`{"mappings":{"properties":{"deleted":{"type":"keyword"},"data":{"type":"object"}}}}`,
		`{"settings":{"default_pipeline":"drop"},"mappings":{"properties":{"deleted":{"type":"boolean"},"data":{"type":"object"}}}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			o := Orchestrator{} // A network call would panic: validation must precede lock acquisition.
			if err := o.Run(context.Background(), "events-v2", json.RawMessage(raw)); err == nil {
				t.Fatal("unsafe definition accepted")
			}
		})
	}
}
func TestAliasMustResolveToOneVersionedIndex(t *testing.T) {
	for _, raw := range []string{`{}`, `{"events-v1":{},"events-v2":{}}`, `{"unexpected":{}}`} {
		t.Run(raw, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, raw) }))
			defer s.Close()
			o := Orchestrator{Client: elastic.New(s.URL)}
			if _, err := o.alias(context.Background(), "events-write"); err == nil {
				t.Fatal("ambiguous alias accepted")
			}
		})
	}
}
