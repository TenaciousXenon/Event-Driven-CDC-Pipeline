# Reindex and recovery runbook

## Normal operation

1. Check connector health, DLQs and ES consumer lag. Fix known missing records
   first: reindex is an ES mapping migration, not a database reconciliation.
2. Edit the mapping file. Preserve the `deleted` boolean and the `data` envelope.
   Leave `_source` enabled; do not configure ingest pipelines that modify/delete
   documents or remove metadata. Only this coordinator may write to the new index.
3. Choose a fresh name such as `events-v2` and run:
   `docker compose --profile tools run --rm reindex -target events-v2`.
4. Confirm both aliases point at the target, and watch `cdc-elasticsearch` lag
   return to normal. Verify application searches. The old index stays write-blocked.
5. Delete old indices only after your validation/retention window and backups.

The durable document `cdc-reindex-locks/_doc/events` is acquired with create-only
semantics. It records source, target, phase, source count and task ID. Competing
runs fail at lock acquisition. It has no expiry: an expired lease would allow an
old coordinator to publish after a newer migration. Treat alias administration
and this lock as privileged operations; outside tools must not bypass it.

The dedicated write-block API waits for in-flight source writes. A refresh then
makes all accepted writes visible to `_reindex`. The source remains immutable
through validation. The target starts empty, must copy the source total with no
failures, conflicts or updates, and must have the same count after refresh.
This validates the copy operation and cardinality, not a separate content hash or
business-semantic equivalence check. Do not attach a transforming ingest pipeline.

Only after those gates does a single `_aliases` request remove both old aliases
and add both new aliases. Kafka messages rejected by the fence are uncommitted.
Retried writes resolve `events-write` again after promotion. A crash after a sink
write but before an offset commit is harmless under the versioning contract.

This intentionally trades ES freshness for a simple correctness boundary.
PostgreSQL, Debezium, Kafka, Redis and searches continue. Size Kafka retention to
cover the entire copy and subsequent catch-up; the source topic must not expire
unprocessed changes. Large datasets may need a future dual-write or checkpointed
Kafka replay implementation with a shorter fence window.

## Failure behavior

Every failure after acquiring the lock keeps the lock. If the source was already
fenced, it stays fenced too. There is no automatic unfence, target deletion,
lock expiry, or rollback. HTTP timeouts do not prove a remote task or alias swap
failed. Inspect reality before taking a recovery action.

```bash
curl -fsS http://localhost:9200/cdc-reindex-locks/_doc/events
curl -fsS http://localhost:9200/_alias/events-read
curl -fsS http://localhost:9200/_alias/events-write
curl -fsS 'http://localhost:9200/_tasks?actions=*reindex&detailed=true'
# Use the task ID stored in the lock, if present:
curl -fsS 'http://localhost:9200/_tasks/NODE:TASK'
```

Stop the coordinator process/container before recovering. If the task request
succeeded but recording its ID failed, find the active task by source/target.
Cancellation is asynchronous: request `POST /_tasks/NODE:TASK/_cancel` and wait
until it has stopped before deleting its target or clearing the lock.
Never delete/recreate the lock index to resolve contention.

### Both aliases still point at the old source

After confirming the coordinator and all associated tasks have stopped:

1. Keep search pointed at the source. The failed target has not been published.
2. Remove the source fence using
   `PUT /SOURCE/_settings` with `{"index.blocks.write":false}`.
3. Confirm the ES worker resumes and consumer lag decreases.
4. Delete the failed **target only** after checking its name and aliases.
5. Delete `cdc-reindex-locks/_doc/events` to release the lock.
6. Fix the issue and rerun with a fresh target name.

Do not issue these mutations using guessed source/target names. Read the lock and
aliases. A failure before the source fence needs only target cleanup, if created,
and lock release after stopping the coordinator.

### Both aliases point at the target

The cutover succeeded even if the CLI reported a timeout or checkpoint failure.
Confirm the target is healthy and writable, and check that the ES worker catches
up. Keep the old source blocked. Once the coordinator is stopped, remove the
leftover lock. Do **not** move the aliases back: the old source no longer contains
new writes accepted after promotion.

### Aliases differ or an unexpected index appears

Stop the ES worker and all coordinators, retain the lock and fences, and reconcile
which index contains acknowledged writes before changing aliases. This indicates
manual interference or a broken operational contract. No automatic recovery is
safe. Preserve data and use a planned full rebuild if necessary.

## Rollback

The old index is a point-in-time snapshot, not a continuously synchronized standby.
A direct alias rollback after writes resume would lose visibility of new changes.
For a mapping rollback, reindex the current index into a fresh index using the old
mapping, with the same fencing procedure. If the old mapping cannot represent
current data, plan a source snapshot/reconciliation instead.
