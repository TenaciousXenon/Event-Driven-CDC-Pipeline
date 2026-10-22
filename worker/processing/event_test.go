package processing

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestDecode(t *testing.T) {
	key := []byte(`{"payload":{"id":9007199254740993}}`)
	good := []byte(`{"payload":{"op":"u","after":{"id":9007199254740993}}}`)
	e, err := Decode(key, good, 40)
	if err != nil || e.ID != "9007199254740993" || e.Version != 41 || e.Data["id"].(json.Number).String() != e.ID {
		t.Fatalf("precision/version: %+v %v", e, err)
	}
	for _, raw := range []string{`null`, `{}`, `{"payload":null}`, `{"payload":{"op":"x"}}`, `{"payload":{"op":"c"}}`, string(good) + ` {}`} {
		if _, err := Decode(key, []byte(raw), 0); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{`{}`, `{"payload":{}}`, `{"payload":{"id":1.2}}`, `{"payload":{"id":9223372036854775808}}`} {
		if _, err := Decode([]byte(raw), good, 0); err == nil {
			t.Errorf("accepted key %s", raw)
		}
	}
	if e, err := Decode(nil, nil, 0); err != nil || e != nil {
		t.Fatal("tombstone")
	}
	if e, err := Decode(key, []byte(`{"payload":{"op":"d"}}`), 2); err != nil || !e.Deleted {
		t.Fatal("delete")
	}
	for _, op := range []string{"c", "r"} {
		if _, err := Decode(key, []byte(`{"payload":{"op":"`+op+`","after":{}}}`), 0); err != nil {
			t.Fatal(err)
		}
	}
}
func TestRetry(t *testing.T) {
	n := 0
	attempts, err := Retry(context.Background(), 5, 0, func() error {
		n++
		if n < 3 {
			return errors.New("temporary")
		}
		return nil
	})
	if attempts != 3 || err != nil {
		t.Fatal(attempts, err)
	}
	attempts, err = Retry(context.Background(), 5, 0, func() error { return Permanent(errors.New("bad")) })
	if attempts != 1 || err == nil {
		t.Fatal(attempts, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts, err = Retry(ctx, 5, time.Second, func() error { t.Fatal("called after cancellation"); return nil })
	if attempts != 0 || !errors.Is(err, context.Canceled) {
		t.Fatal(attempts, err)
	}
}
