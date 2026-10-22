# Validation record

Validated with a workspace-local Go 1.27.1 toolchain:

- `gofmt` on all Go source files: clean.
- `go mod verify`: all modules verified.
- `go vet ./...`: passed.
- `go test -race -count=1 -cover ./...`: passed, including actual Redis integration.
- `go build -buildvcs=false ./...`: passed. VCS stamping was disabled because the
  assembled tree is not a standalone Git checkout in the execution workspace.
- A five-second, two-worker decoder fuzz run: passed, 63,333 generated executions.
- Ten Python harness tests: passed, without Docker.
- Python/JSON/YAML parsing and Bash syntax checks: passed.
- Scenario list/plan commands: exercised without Docker; all 14 toggles available.

A temporary Redis 7.4.2 server was built and run locally to exercise the actual Lua
script. Duplicate/stale writes, persistent delete guards, versions above 2^53 and
128 concurrent out-of-order versions passed under the Go race detector. That
server had no persistent data and was stopped after the tests; this does not
establish the Docker service's AOF restart behavior.

Race-enabled tests cover the commit/DLQ ordering barrier, interrupted retries,
ambiguous commit acknowledgement and redelivery, exact DLQ diagnostic bytes,
permanent/transient error classification, decoder bounds, HTTP behavior and
reindex failures before and after alias publication. Reindex tests use HTTP test
servers and do not establish real-cluster behavior by themselves. The host-side
runner tests cover project isolation, cleanup ordering, interrupted runs, setup
failure, stopping after failures and preventing false pass reports.

The new failure-classification tests identified and fixed a behavior in the prior
version: not every Elasticsearch HTTP 400 is a poison event. Only recognized
mapping/document parsing errors are now quarantined; missing required aliases and
other configuration errors leave source offsets uncommitted.

Docker is not installed in this execution environment. Image builds, Compose
startup, the real Debezium pipeline, live Elasticsearch reindex and the **14 Docker
chaos scenarios were not executed here**. No completed GitHub Actions run is
claimed. Run the integration job and the manual chaos workflow, or use the local
commands in [testing.md](testing.md), before relying on those end-to-end checks.

The suite includes runnable process/service fault scenarios, but does not yet test
asymmetric network partitions, replicated node loss, storage corruption,
rebalance storms, retention/WAL exhaustion or concurrent DDL. It does not claim
exactly-once delivery or cross-sink atomicity.
