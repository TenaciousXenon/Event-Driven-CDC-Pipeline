#!/usr/bin/env bash
# Creates and deletes one test row; uses a dedicated large bigint ID.
set -euo pipefail
cd "$(dirname "$0")/.."
id=9007199254740993
sql() { docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d eventsdb -c "$1" >/dev/null; }
wait_state() {
  local name=$1
  for _ in $(seq 1 90); do
    if NAME="$name" ID="$id" python3 - <<'PY'
import json, os, subprocess, urllib.request
id=os.environ['ID']; name=os.environ['NAME']
try:
    with urllib.request.urlopen('http://localhost:9200/events-read/_search?q=data.id:'+id) as r: hits=json.load(r)['hits']['hits']
    raw=subprocess.check_output(['docker','compose','exec','-T','redis','redis-cli','--raw','HGET','event:'+id,'data']).decode().strip()
    if name=='DELETED': assert not hits and not raw
    else: assert len(hits)==1 and hits[0]['_source']['data']['name']==name and json.loads(raw)['name']==name
except Exception:
    raise SystemExit(1)
PY
    then return; fi
    sleep 2
  done
  echo "Timed out waiting for $name" >&2; exit 1
}
sql "INSERT INTO events(id,name) VALUES ($id,'smoke-created') ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name;"
wait_state smoke-created
sql "UPDATE events SET name='smoke-updated' WHERE id=$id;"
wait_state smoke-updated
# Reindex while CDC continues to be available; target must be new.
docker compose --profile tools run --rm reindex -target "events-vsmoke-$(date +%s)"
wait_state smoke-updated
sql "DELETE FROM events WHERE id=$id;"
wait_state DELETED
echo 'PASS: create, update, bigint precision, reindex and delete in both sinks'
