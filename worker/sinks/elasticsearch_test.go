package sinks

import (
	"context"
	"distributed-cdc/worker/elastic"
	"distributed-cdc/worker/processing"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestElasticDelivery(t *testing.T) {
	for _, status := range []int{200, 201, 409, 400, 403, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PUT" || r.URL.Path != "/events-write/_doc/1" || r.URL.Query().Get("require_alias") != "true" || r.URL.Query().Get("version_type") != "external" {
					t.Error("unsafe write", r.URL)
				}
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				if body["deleted"] != true {
					t.Error("delete must persist a tombstone")
				}
				w.WriteHeader(status)
				if status == 400 {
					w.Write([]byte(`{"error":{"type":"mapper_parsing_exception"}}`))
				}
			}))
			defer server.Close()
			s := Elasticsearch{Client: elastic.New(server.URL), Alias: "events-write"}
			err := s.Apply(context.Background(), processing.Event{ID: "1", Version: 5, Deleted: true})
			if (err != nil) != (status >= 400 && status != 409) {
				t.Fatal(err)
			}
			var permanent *processing.PermanentError
			if errors.As(err, &permanent) != (status == 400) {
				t.Fatal("classification", err)
			}
		})
	}
}

func TestOnlyDocumentErrorsAreQuarantined(t *testing.T) {
	for _, tt := range []struct {
		kind      string
		permanent bool
	}{
		{"mapper_parsing_exception", true}, {"document_parsing_exception", true}, {"strict_dynamic_mapping_exception", true},
		{"index_not_found_exception", false}, {"illegal_argument_exception", false}, {"cluster_block_exception", false}, {"", false},
	} {
		t.Run(tt.kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(400)
				json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"type": tt.kind}})
			}))
			defer server.Close()
			sink := Elasticsearch{Client: elastic.New(server.URL), Alias: "events-write"}
			err := sink.Apply(context.Background(), processing.Event{ID: "1", Version: 1, Deleted: true})
			var permanent *processing.PermanentError
			if err == nil || errors.As(err, &permanent) != tt.permanent {
				t.Fatalf("classification of %s: %v", tt.kind, err)
			}
		})
	}
}
