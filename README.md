distributed-cdc-system/
│
├── compose.yaml
├── .env.example
├── README.md
│
├── debezium/
│   └── postgres-connector.json
│
├── elasticsearch/
│   └── index-mapping.json
│
├── worker/
│   ├── Dockerfile
│   ├── go.mod
│   ├── go.sum
│   ├── main.go
│   │
│   ├── consumer/
│   │   └── reader.go
│   │
│   ├── processing/
│   │   ├── event.go
│   │   ├── decoder.go
│   │   ├── retry.go
│   │   └── dlq.go
│   │
│   ├── sinks/
│   │   ├── sink.go
│   │   ├── elasticsearch.go
│   │   └── redis.go
│   │
│   ├── reindex/
│   │   ├── orchestrator.go
│   │   ├── checkpoint.go
│   │   ├── validator.go
│   │   └── alias.go
│   │
│   └── cmd/
│       └── reindex/
│           └── main.go
│
└── tests/
    ├── integration/
    └── chaos/