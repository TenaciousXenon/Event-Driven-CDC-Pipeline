# Tests and opt-in chaos lab

The test suite has three layers: deterministic Go tests for delivery boundaries,
real Redis checks for the Lua version guard, and selectable Docker fault scenarios
with offset, DLQ, row-state and alias assertions. Faults do not run on ordinary
push/PR CI. A separate manual GitHub Actions workflow runs the selected chaos case.

## Fast checks

From the repository root, with Go 1.27.1 and Python 3.10+ installed:

```bash
bash scripts/test.sh unit
bash scripts/test.sh fuzz          # 20 seconds, two fuzz workers
FUZZ_TIME=60s bash scripts/test.sh fuzz
```

The unit command runs the Python harness's own isolation/cleanup/report tests,
`go vet`, and Go tests with the race detector. Fuzz seeds also run with ordinary
`go test`; active mutation fuzzing is explicitly bounded by `FUZZ_TIME`.

To include the actual Redis integration tests, start a disposable Redis server
and set `REDIS_TEST_ADDR` when running the Go suite. The normal CI job supplies
this service automatically. These tests use uniquely prefixed keys and delete
only those keys; they never issue FLUSHDB/FLUSHALL.

```bash
cd worker
REDIS_TEST_ADDR=localhost:6379 go test -race -count=1 ./sinks
```

Native race tests need a working C compiler. Docker chaos tests need Docker Engine
with Compose v2 supporting `config --format json` and `up --wait`, Bash, and Python
3.10+. Go is built inside containers for chaos runs. On Linux, configure
`vm.max_map_count` for Elasticsearch as described in the main README. Allow enough
memory for a second full stack if the normal development stack is also running.

## Toggle the scenarios

Listing and planning need neither Docker nor running services:

```bash
python3 tests/chaos/run.py list
python3 tests/chaos/run.py plan --all --exclude broker-outage
```

Run one, several, or all cases:

```bash
# Starts an isolated lab automatically on the first run.
bash scripts/test.sh chaos --scenario redis-outage
bash scripts/test.sh chaos --scenario worker-crash --iterations 5 --seed 42
bash scripts/test.sh chaos --scenario poison-pill --scenario dlq-blocked
bash scripts/test.sh chaos --scenario reindex-live --scenario reindex-failure
bash scripts/test.sh chaos --all --exclude broker-outage --duration 10
```

`--duration` controls fault hold time (default 8 seconds, 1..120).
`--timeout` controls each convergence assertion (default 180 seconds, 20..900).
`--iterations` controls crash repetitions (default 3, 1..20).
`--seed` reproduces the randomized crash delay sequence, not Docker scheduling.
A full run can take several minutes plus image builds. The DLQ-unavailable case
waits for an actual worker failure, which can outlast the nominal hold duration.

The runner intentionally stops at the first failure. A restored dependency and
an assertion failure are different outcomes: the latter still fails the report.
No scenario reports a pass solely because a container restarted successfully.

## Isolation and cleanup

The default Compose project is **cdc-chaos**. To run independent labs, choose
`--project cdc-chaos-mytest`; every command for that lab must use the same name.
Other project names are rejected. The runner resolves the repository's Compose
configuration with that project name, verifies that volumes and networks belong
to it, removes all host ports, and adds a test-only probe container. That probe
uses stdin/stdout and has no exposed listener or Docker socket. Its binary is not
included in the production worker image.

The runner does not call the normal bootstrap script: it initializes the isolated
stack itself. It never joins your normal development project's network or volumes.
Tests create unique source rows, extra DLQ-test topics and versioned indices.
Successful runs delete their own source rows, leaving version tombstones. Reindex
cases retain old indices, as the application normally does. The test report and
logs remain under `.chaos/<project>/` (ignored by Git).

Temporary pause/alias faults have registered cleanup handlers. Ctrl-C and SIGTERM
attempt restoration and produce a failed/interrupted report. Failed cleanup is
also a test failure. SIGKILL of the runner, Docker daemon failure or machine loss
cannot run those handlers. Inspect before continuing:

```bash
python3 tests/chaos/run.py status
python3 tests/chaos/run.py recover
```

`recover` only unpauses/starts the lab's standard services. It deliberately does
not clear reindex locks, undo a cutover, or assume a server-side task stopped.
For an unresolved migration, inspect the generated lab Compose config and follow
[the reindex runbook](reindex.md), running requests through the lab probe rather
than localhost (this lab has no host ports). For example:

```bash
printf '%s\n' '{"action":"http","service":"elasticsearch","method":"GET","path":"/cdc-reindex-locks/_doc/events"}' |
  docker compose -p cdc-chaos -f .chaos/cdc-chaos/compose.json --profile tools \
    exec -T probe /bin/testprobe
```

A failed-copy scenario performs controlled recovery only after the coordinator
exits, its exact task is confirmed completed, the lock's source/target/task match,
and both aliases still point at the source. It never guesses how to recover an
ambiguous or active task.

Stop the lab while retaining data, or explicitly erase the disposable data:

```bash
python3 tests/chaos/run.py down
python3 tests/chaos/run.py down --volumes
```

These commands operate only on the named chaos project. No test deletes normal
stack volumes, truncates source tables, or purges all Redis keys.

## Scenario expectations

| Toggle | Injected fault / stimulus | Assertions |
|---|---|---|
| `baseline` | PostgreSQL create, update, delete | Both sinks reach exact names/IDs; ES read searches exclude deleted rows; Redis retains deletion version but removes data. |
| `redis-outage` | Pause Redis | ES processes a new row; Redis's committed source offset stays fixed throughout the observed fault; neither DLQ grows; Redis catches up after unpause. |
| `elasticsearch-outage` | Pause ES | Redis progresses; ES commits stay fixed; no outage DLQ; ES catches up. |
| `broker-outage` | Pause Redpanda while PostgreSQL accepts a row | After recovery both groups catch up and both sinks contain the row; no new DLQ records. |
| `debezium-outage` | Pause connector while PostgreSQL accepts a row | Kafka consumer offsets do not advance during this controlled single-writer pause; WAL catch-up reaches both sinks afterward. |
| `worker-crash` | Pause workers to build a backlog; unpause and SIGKILL; restart repeatedly | Pending rows reach both sinks and groups drain afterward; no fault-only DLQ. The precise killed instruction is intentionally unspecified. |
| `poison-pill` | Publish malformed raw bytes, then a valid PostgreSQL row | Both DLQs preserve exact original key/value bytes and source position; both groups move past the poison and process later valid data. |
| `dlq-blocked` | Dedicated source/group, nonexistent DLQ, poison followed by a tombstone | Observe an actual failed DLQ publish; no committed offset passes the poison or later message. The temporary container is removed by returned container ID. |
| `version-replay` | Apply equal/older versions directly through real sink adapters, including after a DB delete | Existing data is not overwritten and tombstones are not resurrected. This is a sink guard test, not republishing older data at a newer Kafka offset. |
| `sink-restart` | Graceful restart of Redis and ES | Existing values persist; later CDC updates still reach both sinks. |
| `alias-missing` | Temporarily remove only the write alias | ES commits remain fixed, Redis progresses, and no concrete `events-write` index appears; restoring the alias allows catch-up. |
| `reindex-live` | Mapping migration while a background PostgreSQL workload runs | Single-response alias snapshots always show one shared read/write target with the correct filter; target is promoted; delete versions survive; final rows converge. |
| `reindex-failure` | Incompatible numeric mapping for nonnumeric source names | No promotion; completed failed task, lock and write fence remain; Redis progresses while ES commits stay fixed; verified recovery permits catch-up. |
| `reindex-lock` | Existing coordinator lock | Competitor exits unsuccessfully, leaves aliases unchanged, and creates no target index. |

All successful non-poison cases additionally check that the main sink DLQs did not
grow, and that both groups end at the source high watermark. Reports include
before/after committed offsets, DLQ end offsets, alias targets and scenario errors.
Failure logs are captured separately. Topic partition count is one in this lab,
which makes the offset barrier assertions exact and observable.

## Deterministic test coverage

The Go tests exercise boundaries that are hard to hit reliably with a container
kill: sink-success/commit-failure/redelivery, acknowledged-DLQ/commit-failure,
failed-DLQ/no-commit, stopping before a later partition offset is committed,
shutdown before handling and during retry backoff, permanent versus transient
errors, and preservation of exact DLQ identity/bytes.

Reindex tests inject failures at lock acquisition, target creation, checkpoints,
write fence, refresh, count/shard validation, copy task, alias transport/acknowledgement,
and the checkpoint after promotion. They verify that unsafe states never release
the lock. Task validation covers missing responses, timeouts, conflicts, unexpected
updates, partial counts, failure arrays, polling, cancellation and zero-row copies.
Mapping and alias validation tests reject unsafe source configurations.

Redis integration tests use versions above 2^53 and race 128 out-of-order writes
against the expected maximum-version model. Decoder tests cover bigint boundaries,
invalid offsets, malformed envelopes, snapshot/create/update/delete, tombstones
and fuzzed inputs. HTTP tests check bounded error bodies and cancellation.

## Manual GitHub Actions run

Select **Actions → Opt-in chaos tests → Run workflow**, choose one scenario or
`all`, and set the hold time / crash iterations. The job uploads `chaos-results`
with JSON reports and logs even after scenario failures, then removes its lab.
Normal push/PR CI runs unit tests, Redis integration, bounded fuzzing, host-harness
checks, and the existing Docker smoke job; it does not inject these fault scenarios.

## What these tests do not establish

Service pause simulates a nonresponding dependency, not an asymmetric network
partition. Graceful sink restart tests configured persistence, not power-loss or
corrupt-disk recovery. Worker crash timing is not a deterministic commit-boundary
injection; the unit tests cover those logical boundaries. Reindex alias snapshots
are sampled, while the single-request alias operation provides the underlying
atomicity. Content checks concern the run's fixtures, not a whole-database hash.

There are no claims of exactly-once delivery, cross-sink transactions, replicated
node-loss resilience, bounded recovery after retention expires, automatic schema
migration, or automatic failed-reindex recovery. Multi-partition rebalance storms,
asymmetric packet loss, storage corruption, OOM and WAL/retention exhaustion remain
separate tests to add. Do not infer those properties from a green lab run.

See [validation.md](validation.md) for checks actually executed in the authoring
environment; shipping runnable scenarios is not the same as claiming they passed.
