# Validation record

Validated with a workspace-local Go 1.27.1 toolchain:

- `gofmt` on all Go source files: clean.
- `go mod verify`: all modules verified.
- `go vet ./...`: passed.
- `go test -race -count=1 -cover ./...`: passed.
- `go build -buildvcs=false ./...`: passed. VCS stamping was disabled because this
  assembled source tree is not a standalone Git checkout in the execution workspace.
- JSON and YAML parsing: passed.
- Bash syntax checks for bootstrap and smoke scripts: passed.

Race-enabled tests cover decoder validation and large integers, retry and
cancellation, commit/DLQ barriers, HTTP write/version/delete semantics, reindex
lock contention, failed write fences, failed copy tasks, count mismatches and
alias request failure. Reindex tests use an HTTP test server; they do not establish
real-cluster behavior by themselves.

The Redis integration test is skipped locally because no Redis server is
available. The CI unit job supplies a Redis service and sets `REDIS_TEST_ADDR` to
exercise the actual Lua script, duplicate/stale writes, deletes, and IDs/versions
above JavaScript/Lua's exact floating-point integer range.

Docker is not installed in this execution environment. Container image builds,
Compose startup, the real Debezium pipeline, and live Elasticsearch reindex were
**not executed here**. The included GitHub Actions integration job builds the
images, bootstraps a fresh stack and runs `scripts/smoke.sh`. Run that job or the
same scripts locally before tagging a release. No CI run is claimed to have
completed, and no GitHub repository has been created or pushed.

Not yet covered by automated end-to-end checks: broker/node loss, process kills
at every commit boundary, rebalance storms, retention exhaustion, and concurrent
DDL/schema changes. The current implementation documents those operational
limits rather than claiming production fault tolerance.
