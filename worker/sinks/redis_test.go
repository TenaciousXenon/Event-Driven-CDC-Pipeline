package sinks

import (
	"context"
	"distributed-cdc/worker/processing"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"testing"
	"time"
)

func TestRedisVersionGuard(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR for real Redis integration test")
	}
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	prefix := fmt.Sprintf("cdc-test:%d:", time.Now().UnixNano())
	s := Redis{Client: c, Prefix: prefix}
	defer c.Del(ctx, prefix+"1")
	apply := func(v int64, deleted bool, name string) {
		t.Helper()
		if err := s.Apply(ctx, processing.Event{ID: "1", Version: v, Deleted: deleted, Data: map[string]any{"name": name}}); err != nil {
			t.Fatal(err)
		}
	}
	apply(9007199254740993, false, "current")
	apply(9007199254740992, false, "stale")
	apply(9007199254740993, false, "duplicate")
	if got := c.HGet(ctx, prefix+"1", "data").Val(); got != `{"name":"current"}` {
		t.Fatal(got)
	}
	apply(9007199254740994, true, "")
	apply(9007199254740993, false, "resurrect")
	if c.HExists(ctx, prefix+"1", "data").Val() || c.HGet(ctx, prefix+"1", "deleted").Val() != "true" {
		t.Fatal("delete resurrected")
	}
	apply(9007199254740995, false, "recreated")
	if c.HGet(ctx, prefix+"1", "data").Val() != `{"name":"recreated"}` {
		t.Fatal("new create rejected")
	}
}

func TestRedisConcurrentVersionsMatchMonotonicModel(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR for real Redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	prefix := fmt.Sprintf("cdc-model:%d:", time.Now().UnixNano())
	s := Redis{Client: c, Prefix: prefix}
	defer c.Del(context.Background(), prefix+"1")
	errs := make(chan error, 128)
	const base int64 = 9007199254740992
	for i := 1; i <= 128; i++ {
		go func(n int) {
			errs <- s.Apply(ctx, processing.Event{ID: "1", Version: base + int64(n), Deleted: n%3 == 0, Data: map[string]any{"name": fmt.Sprint(n)}})
		}(i)
	}
	for i := 0; i < 128; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	row, err := c.HGetAll(ctx, prefix+"1").Result()
	if err != nil {
		t.Fatal(err)
	}
	if row["version"] != "9007199254741120" || row["deleted"] != "false" || row["data"] != `{"name":"128"}` {
		t.Fatal("concurrent writes diverged from maximum-version model", row)
	}
}
