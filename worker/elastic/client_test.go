package elastic

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBoundedErrorBody(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte(strings.Repeat("x", 20000)))
	}))
	defer s.Close()
	err := New(s.URL+"/").Do(context.Background(), "GET", "/test", nil, nil)
	var h *HTTPError
	if !errors.As(err, &h) || h.Status != 503 || len(h.Body) != 8192 {
		t.Fatalf("unbounded or unclassified error: %v", err)
	}
}
func TestCancelledRequest(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := New(s.URL).Do(ctx, "GET", "/test", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestMalformedSuccessfulResponse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("not JSON")) }))
	defer s.Close()
	var out map[string]any
	if err := New(s.URL).Do(context.Background(), "GET", "/test", nil, &out); err == nil {
		t.Fatal("invalid response accepted")
	}
}
