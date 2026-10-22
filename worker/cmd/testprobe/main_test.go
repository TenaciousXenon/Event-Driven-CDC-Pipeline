package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeDoesNotSendNullBodyToBodylessElasticsearchAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if len(raw) != 0 || r.ContentLength != 0 {
			t.Errorf("GET aliases must be bodyless, got %q", raw)
		}
		w.Write([]byte(`{"id":9007199254740993}`))
	}))
	defer server.Close()
	for _, body := range []json.RawMessage{nil, json.RawMessage(`null`)} {
		value, err := requestHTTP(context.Background(), server.URL, request{Method: "GET", Path: "/_alias/events-write", Body: body})
		if err != nil {
			t.Fatal(err)
		}
		response := value.(map[string]any)["body"].(map[string]any)
		if response["id"].(json.Number).String() != "9007199254740993" {
			t.Fatal("probe rounded a bigint")
		}
	}
}
func TestProbePreservesRequestAndErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if string(raw) != `{"id":9007199254740993}` {
			t.Errorf("request body changed: %s", raw)
		}
		w.WriteHeader(409)
		w.Write([]byte(`{"error":"conflict"}`))
	}))
	defer server.Close()
	value, err := requestHTTP(context.Background(), server.URL, request{Method: "PUT", Path: "/index", Body: json.RawMessage(`{"id":9007199254740993}`)})
	if err != nil || value.(map[string]any)["status"] != 409 {
		t.Fatal(value, err)
	}
}
