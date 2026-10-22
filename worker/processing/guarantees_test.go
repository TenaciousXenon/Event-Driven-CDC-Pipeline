package processing

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestOffsetBoundsAndIDBounds(t *testing.T) {
	value := []byte(`{"payload":{"op":"d"}}`)
	for _, id := range []string{"0", "-1", "-9223372036854775808", "9223372036854775807"} {
		e, err := Decode([]byte(`{"payload":{"id":`+id+`}}`), value, math.MaxInt64-1)
		if err != nil || e.ID != id || e.Version != math.MaxInt64 {
			t.Fatal(e, err)
		}
	}
	for _, offset := range []int64{-1, math.MaxInt64} {
		if _, err := Decode([]byte(`{"payload":{"id":1}}`), value, offset); err == nil {
			t.Fatal("invalid version accepted")
		}
	}
}
func TestRetryCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := Retry(ctx, 5, time.Hour, func() error { close(called); return errors.New("offline") })
		done <- err
	}()
	<-called
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("backoff ignored shutdown")
	}
}
func TestRetryExhaustionAndInvalidPolicy(t *testing.T) {
	calls := 0
	n, err := Retry(context.Background(), 4, 0, func() error { calls++; return errors.New("temporary") })
	if err == nil || n != 4 || calls != 4 {
		t.Fatal(n, calls, err)
	}
	n, err = Retry(context.Background(), 0, 0, func() error { t.Fatal("invalid retry policy invoked callback"); return nil })
	if n != 0 || err == nil {
		t.Fatal(n, err)
	}
}
func FuzzDecodeNeverPanics(f *testing.F) {
	f.Add([]byte(`{"payload":{"id":1}}`), []byte(`{"payload":{"op":"u","after":{"id":1}}}`), int64(1))
	f.Add([]byte(`null`), []byte(`null`), int64(-1))
	f.Add([]byte(`{}`), []byte{}, int64(0))
	f.Fuzz(func(t *testing.T, key, value []byte, offset int64) {
		e, err := Decode(key, value, offset)
		if err == nil && e != nil && (e.Version <= 0 || e.ID == "" || (!e.Deleted && e.Data == nil)) {
			t.Fatal("invalid successful event", e)
		}
	})
}
