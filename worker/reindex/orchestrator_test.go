package reindex

import (
	"context"
	"distributed-cdc/worker/elastic"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFencedCutover(t *testing.T) {
	for _, failure := range []string{"", "lock", "create", "checkpoint", "block", "refresh", "task", "count", "count-shard", "swap", "swap-unacknowledged", "swap-errors", "promoted-checkpoint"} {
		t.Run("failure_"+failure, func(t *testing.T) {
			blocked, copied, swapped, unlocked := false, false, false, false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.Path
				w.Header().Set("Content-Type", "application/json")
				switch {
				case path == "/cdc-reindex-locks/_create/events":
					if failure == "lock" {
						w.WriteHeader(409)
						fmt.Fprint(w, `{}`)
						return
					}
					fmt.Fprint(w, `{}`)
				case strings.HasPrefix(path, "/_alias/"):
					fmt.Fprint(w, `{"events-v1":{"aliases":{}}}`)
				case path == "/cdc-reindex-locks/_update/events":
					var checkpoint struct {
						Doc struct {
							Phase string `json:"phase"`
						} `json:"doc"`
					}
					json.NewDecoder(r.Body).Decode(&checkpoint)
					if failure == "checkpoint" || (failure == "promoted-checkpoint" && checkpoint.Doc.Phase == "promoted") {
						w.WriteHeader(503)
						fmt.Fprint(w, `{}`)
						return
					}
					fmt.Fprint(w, `{}`)
				case path == "/events-v2" && r.Method == "PUT":
					if failure == "create" {
						w.WriteHeader(400)
						fmt.Fprint(w, `{}`)
						return
					}
					fmt.Fprint(w, `{}`)
				case path == "/events-v1/_block/write":
					if failure == "block" {
						fmt.Fprint(w, `{"acknowledged":false}`)
						return
					}
					blocked = true
					fmt.Fprint(w, `{"acknowledged":true,"shards_acknowledged":true}`)
				case strings.HasSuffix(path, "/_refresh"):
					if !blocked {
						t.Error("refresh before fence")
					}
					if failure == "refresh" {
						fmt.Fprint(w, `{"_shards":{"failed":1}}`)
						return
					}
					fmt.Fprint(w, `{"_shards":{"failed":0}}`)
				case strings.HasSuffix(path, "/_count"):
					if failure == "count-shard" {
						fmt.Fprint(w, `{"count":2,"_shards":{"failed":1}}`)
						return
					}
					count := 2
					if failure == "count" && strings.HasPrefix(path, "/events-v2/") {
						count = 1
					}
					fmt.Fprintf(w, `{"count":%d,"_shards":{"failed":0}}`, count)
				case path == "/_reindex":
					if !blocked {
						t.Error("copy before fence")
					}
					var body struct {
						Source struct{ Index string }
						Dest   struct {
							Version string `json:"version_type"`
						}
					}
					json.NewDecoder(r.Body).Decode(&body)
					if body.Source.Index != "events-v1" || body.Dest.Version != "external" {
						t.Error("copy must use physical source and preserve versions")
					}
					copied = true
					fmt.Fprint(w, `{"task":"node:1"}`)
				case path == "/_tasks/node:1":
					failures := "[]"
					if failure == "task" {
						failures = `[{"cause":"bad mapping"}]`
					}
					fmt.Fprintf(w, `{"completed":true,"response":{"total":2,"created":2,"failures":%s}}`, failures)
				case path == "/_aliases":
					if !blocked || !copied {
						t.Error("swap before copy")
					}
					var body struct{ Actions []map[string]map[string]any }
					json.NewDecoder(r.Body).Decode(&body)
					if len(body.Actions) != 4 {
						t.Error("aliases must swap together")
					}
					if body.Actions[0]["remove"]["must_exist"] != true || body.Actions[3]["add"]["is_write_index"] != true {
						t.Error("unsafe alias request")
					}
					if failure == "swap" {
						w.WriteHeader(503)
						fmt.Fprint(w, `{}`)
						return
					}
					swapped = true
					if failure == "swap-unacknowledged" {
						fmt.Fprint(w, `{"acknowledged":false}`)
						return
					}
					if failure == "swap-errors" {
						fmt.Fprint(w, `{"acknowledged":true,"errors":true}`)
						return
					}
					fmt.Fprint(w, `{"acknowledged":true}`)
				case path == lockPath && r.Method == "DELETE":
					unlocked = true
					fmt.Fprint(w, `{}`)
				default:
					t.Errorf("unexpected %s %s", r.Method, path)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			o := Orchestrator{Client: elastic.New(server.URL)}
			err := o.Run(context.Background(), "events-v2", json.RawMessage(`{"mappings":{"properties":{"deleted":{"type":"boolean"},"data":{"type":"object"}}}}`))
			if (err != nil) != (failure != "") {
				t.Fatalf("err=%v", err)
			}
			if failure == "" {
				if !swapped || !unlocked {
					t.Fatal("not promoted")
				}
			} else {
				mayHaveSwapped := failure == "swap-unacknowledged" || failure == "swap-errors" || failure == "promoted-checkpoint"
				if unlocked || swapped != mayHaveSwapped {
					t.Fatal("failure released lock or crossed unexpected cutover boundary")
				}
			}
		})
	}
}
func TestInvalidTargetDoesNotMakeRequests(t *testing.T) {
	o := Orchestrator{}
	if err := o.Run(context.Background(), "../_all", json.RawMessage(`{}`)); err == nil {
		t.Fatal("accepted unsafe name")
	}
}
