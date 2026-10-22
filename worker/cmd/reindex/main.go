package main

import (
	"context"
	"distributed-cdc/worker/elastic"
	"distributed-cdc/worker/reindex"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	target := flag.String("target", "", "new physical index, e.g. events-v2")
	mapping := flag.String("mapping", "/config/index-mapping.json", "index definition JSON")
	endpoint := flag.String("url", "http://elasticsearch:9200", "Elasticsearch URL")
	timeout := flag.Duration("timeout", 30*time.Minute, "overall job deadline")
	flag.Parse()
	if *target == "" {
		log.Fatal("-target is required")
	}
	raw, err := os.ReadFile(*mapping)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	o := reindex.Orchestrator{Client: elastic.New(*endpoint)}
	if err = o.Run(ctx, *target, raw); err != nil {
		log.Fatalf("reindex stopped: %v; inspect durable lock, task and aliases using docs/reindex.md", err)
	}
	log.Printf("promoted %s; old index retained and write-blocked", *target)
}
