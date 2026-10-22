#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Start infrastructure first; no worker can write until aliases exist.
docker compose up -d postgres redpanda elasticsearch redis debezium
for service in http://localhost:9200 http://localhost:8083/connectors; do
  ready=false
  for _ in $(seq 1 90); do
    if curl -fsS "$service" >/dev/null; then ready=true; break; fi
    sleep 2
  done
  if [[ "$ready" != true ]]; then echo "Timed out waiting for $service" >&2; exit 1; fi
done
for topic in app.public.events app.public.events.elasticsearch.dlq app.public.events.redis.dlq; do
  if ! docker compose exec -T redpanda rpk topic describe "$topic" >/dev/null 2>&1; then
    docker compose exec -T redpanda rpk topic create "$topic" -p 1 -r 1 \
      -c cleanup.policy=delete -c retention.ms=604800000
  fi
done

# A fresh namespace is required; never silently migrate an existing hard-delete demo.
status=$(curl -sS -o /dev/null -w '%{http_code}' http://localhost:9200/_alias/events-write)
if [[ "$status" == 404 ]]; then
  python3 - <<'PY' | curl -fsS -X PUT http://localhost:9200/events-v1 -H 'Content-Type: application/json' --data-binary @-
import json
with open('elasticsearch/index-mapping.json') as f: body=json.load(f)
body['aliases']={'events-write':{'is_write_index':True},'events-read':{'filter':{'term':{'deleted':False}}}}
print(json.dumps(body))
PY
elif [[ "$status" != 200 ]]; then
  echo "Cannot inspect aliases: HTTP $status" >&2; exit 1
fi
status=$(curl -sS -o /dev/null -w '%{http_code}' http://localhost:9200/cdc-reindex-locks)
if [[ "$status" == 404 ]]; then
  curl -fsS -X PUT http://localhost:9200/cdc-reindex-locks -H 'Content-Type: application/json' \
    -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}'
elif [[ "$status" != 200 ]]; then
  echo "Cannot inspect reindex lock index: HTTP $status" >&2; exit 1
fi
python3 - <<'PY' | curl -fsS -X PUT http://localhost:8083/connectors/postgres-cdc/config -H 'Content-Type: application/json' --data-binary @-
import json
with open('debezium/postgres-connector.json') as f: print(json.dumps(json.load(f)['config']))
PY
docker compose up -d --build worker-elasticsearch worker-redis
echo 'Started. Check connector status and run scripts/smoke.sh.'
