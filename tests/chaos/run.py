#!/usr/bin/env python3
"""Opt-in, isolated Docker fault tests. Standard library only; see docs/testing.md."""
from __future__ import annotations
import argparse
import base64
import contextlib
import json
import os
from pathlib import Path
import random
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
import uuid

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = {
    "baseline": "PostgreSQL create/update/delete reaches both sinks",
    "redis-outage": "Paused Redis stalls only its group; no outage DLQ; catch-up",
    "elasticsearch-outage": "Paused Elasticsearch stalls only its group; catch-up",
    "broker-outage": "Paused broker buffers source changes; recover and converge",
    "debezium-outage": "Paused connector catches up from PostgreSQL WAL",
    "worker-crash": "SIGKILL workers with backlog; restart and converge",
    "poison-pill": "Bad bytes preserved in each DLQ; later records progress",
    "dlq-blocked": "Missing DLQ prevents poison offset/later offset commit",
    "version-replay": "Equal/older sink writes cannot regress rows or resurrect deletes",
    "sink-restart": "Redis AOF and Elasticsearch survive graceful service restarts",
    "alias-missing": "Missing write alias cannot auto-create a physical sink index",
    "reindex-live": "Atomic alias snapshot, writes during migration, final convergence",
    "reindex-failure": "Incompatible mapping retains lock/fence, Redis progresses, recovery",
    "reindex-lock": "Competing coordinator rejected before index creation/fence",
}
SERVICES = ("postgres", "redpanda", "elasticsearch", "redis", "debezium",
            "worker-elasticsearch", "worker-redis")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def select_scenarios(args):
    names = list(SCENARIOS) if args.all else (args.scenario or ["baseline"])
    return list(dict.fromkeys(n for n in names if n not in args.exclude))


def poll(fn, timeout, description, interval=1):
    deadline = time.monotonic() + timeout
    last = None
    while True:
        try:
            result = fn()
            if result:
                return result
        except (AssertionError, RuntimeError, ValueError, KeyError, OSError) as exc:
            last = str(exc)
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out: {description}; last observation: {last}")
        time.sleep(min(interval, max(0, deadline - time.monotonic())))


class Lab:
    def __init__(self, args):
        self.args = args
        self.project = args.project
        require(re.fullmatch(r"cdc-chaos(?:-[a-z0-9]+)*", self.project),
                "project must be cdc-chaos or cdc-chaos-<lowercase suffix>")
        self.state = ROOT / ".chaos" / self.project
        self.config = self.state / "compose.json"
        self.token = uuid.uuid4().hex[:12]
        self.rng = random.Random(args.seed)
        self.ids = []
        self.expected = {}
        self.cleanups = []
        self.commands = []
        self.report = {"project": self.project, "run": self.token, "seed": args.seed,
                       "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "scenarios": [], "cleanup_errors": []}

    def sh(self, argv, *, data=None, check=True, timeout=180):
        self.commands.append(list(argv))
        result = subprocess.run(argv, input=data, text=True, capture_output=True,
                                cwd=ROOT, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(argv)}\n"
                               f"{result.stdout[-4000:]}\n{result.stderr[-4000:]}")
        return result

    def dc(self, *args, **kw):
        return self.sh(["docker", "compose", "-p", self.project, "-f", str(self.config),
                        "--profile", "tools", *args], **kw)

    def probe(self, action, **fields):
        result = self.dc("exec", "-T", "probe", "/bin/testprobe",
                         data=json.dumps({"action": action, **fields}), timeout=25)
        return json.loads(result.stdout)

    def http(self, method, path, body=None, *, service="elasticsearch", status=(200, 201)):
        result = self.probe("http", method=method, path=path, body=body, service=service)
        require(result["status"] in status,
                f"{method} {path}: HTTP {result['status']}: {result['body']}")
        return result["body"]

    def prepare_config(self):
        self.state.mkdir(parents=True, exist_ok=True)
        result = self.sh(["docker", "compose", "-p", self.project, "-f", str(ROOT / "compose.yaml"),
                          "--profile", "tools", "config", "--format", "json"])
        config = json.loads(result.stdout)
        config["name"] = self.project
        for service in config["services"].values():
            service.pop("ports", None)  # No collisions with a normal dev stack.
        # Docker resolves all paths and project-owned resource names before we edit.
        for volume in config.get("volumes", {}).values():
            require(not volume.get("external"), "external volumes are not permitted")
            require(volume.get("name", "").startswith(self.project + "_"),
                    "refusing a volume outside the chaos project")
        for network in config.get("networks", {}).values():
            require(not network.get("external"), "external networks are not permitted")
            require(network.get("name", "").startswith(self.project + "_"),
                    "refusing a network outside the chaos project")
        config["services"]["probe"] = {
            "build": {"context": str(ROOT / "worker"), "dockerfile": "Dockerfile.test"},
            "init": True, "profiles": ["tools"],
        }
        bad = json.loads((ROOT / "elasticsearch/index-mapping.json").read_text())
        bad["mappings"]["properties"]["data"]["properties"]["name"] = {"type": "long"}
        mapping_path = self.state / "bad-mapping.json"
        mapping_path.write_text(json.dumps(bad))
        config["services"]["reindex"].setdefault("volumes", []).append({
            "type": "bind", "source": str(mapping_path),
            "target": "/config/chaos-bad-mapping.json", "read_only": True})
        self.config.write_text(json.dumps(config, indent=2))

    def setup(self):
        self.prepare_config()
        self.dc("build", "worker-elasticsearch", "worker-redis", "reindex", "probe", timeout=600)
        self.dc("up", "-d", "--wait", "postgres", "redpanda", "elasticsearch", "redis", "probe", timeout=240)
        for topic in ("app.public.events", *self.dlqs()):
            self.create_topic(topic)
        if self.probe("http", service="elasticsearch", method="GET", path="/_alias/events-write")["status"] == 404:
            mapping = json.loads((ROOT / "elasticsearch/index-mapping.json").read_text())
            mapping["aliases"] = {
                "events-read": {"filter": {"term": {"deleted": False}}},
                "events-write": {"is_write_index": True}}
            self.http("PUT", "/events-v1", mapping)
        if self.probe("http", service="elasticsearch", method="GET", path="/cdc-reindex-locks")["status"] == 404:
            self.http("PUT", "/cdc-reindex-locks", {"settings": {"number_of_shards": 1, "number_of_replicas": 0}})
        lock = self.probe("http", service="elasticsearch", method="GET", path="/cdc-reindex-locks/_doc/events")
        require(lock["status"] == 404, "unresolved reindex lock: inspect this lab before running more faults")
        self.dc("up", "-d", "debezium")
        poll(lambda: self.http("GET", "/connectors", service="debezium") is not None,
             self.args.timeout, "connector REST API")
        connector = json.loads((ROOT / "debezium/postgres-connector.json").read_text())
        self.http("PUT", "/connectors/postgres-cdc/config", connector["config"], service="debezium")
        poll(lambda: self.connector_ready(), self.args.timeout, "connector task RUNNING")
        self.dc("up", "-d", "worker-elasticsearch", "worker-redis")
        self.row("setup-canary")
        self.drain()

    def connector_ready(self):
        state = self.http("GET", "/connectors/postgres-cdc/status", service="debezium")
        return (state["connector"]["state"] == "RUNNING" and state["tasks"]
                and all(task["state"] == "RUNNING" for task in state["tasks"]))

    def create_topic(self, topic):
        existing = self.dc("exec", "-T", "redpanda", "rpk", "topic", "describe", topic, check=False)
        if existing.returncode:
            self.dc("exec", "-T", "redpanda", "rpk", "topic", "create", topic, "-p", "1", "-r", "1",
                    "-c", "cleanup.policy=delete", "-c", "retention.ms=604800000")

    @staticmethod
    def dlqs():
        return [f"app.public.events.{sink}.dlq" for sink in ("elasticsearch", "redis")]

    def bounds(self, topic):
        return self.probe("bounds", topic=topic)["last"]

    def offsets(self):
        return {sink: self.probe("offset", topic="app.public.events", group="cdc-" + sink)["offset"]
                for sink in ("elasticsearch", "redis")}

    def drain(self):
        def caught_up():
            end = self.bounds("app.public.events")
            return all(offset == end for offset in self.offsets().values())
        poll(caught_up, self.args.timeout, "both groups at source high watermark")

    def sql(self, sql):
        return self.dc("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "app",
                       "-d", "eventsdb", "-c", sql)

    def row(self, name, *, wait=True, sinks=("elasticsearch", "redis")):
        ident = -int(uuid.uuid4().hex[:14], 16)  # Unique signed bigint, no fixture collisions.
        self.ids.append(ident)
        self.update(ident, name, insert=True)
        if wait:
            self.wait_row(ident, sinks=sinks)
        return ident

    def update(self, ident, name, *, insert=False):
        quoted = name.replace("'", "''")
        if insert:
            self.sql(f"INSERT INTO events(id,name) VALUES ({ident},'{quoted}');")
        else:
            self.sql(f"UPDATE events SET name='{quoted}' WHERE id={ident};")
        self.expected[ident] = name

    def delete(self, ident):
        self.sql(f"DELETE FROM events WHERE id={ident};")
        self.expected[ident] = None

    def es_doc(self, ident):
        return self.http("GET", f"/events-write/_doc/{ident}")

    def check_row(self, ident, sinks=("elasticsearch", "redis")):
        name = self.expected[ident]
        if "elasticsearch" in sinks:
            document = self.es_doc(ident)
            require(document["_source"]["deleted"] == (name is None), "ES deletion differs")
            if name is not None:
                require(document["_source"]["data"]["name"] == name, "ES row differs")
                require(document["_source"]["data"]["id"] == ident, "ES ID precision lost")
            else:
                hits = self.http("POST", "/events-read/_search", {"query": {"ids": {"values": [str(ident)]}}})
                require(not hits["hits"]["hits"], "read alias exposed a tombstone")
        if "redis" in sinks:
            row = self.probe("redis", key="event:" + str(ident))
            require(row.get("deleted") == str(name is None).lower(), "Redis deletion differs")
            if name is not None:
                data = json.loads(row["data"])
                require(data["name"] == name and data["id"] == ident, "Redis row differs")
            else:
                require("data" not in row, "deleted Redis hash retained data")
        return True

    def wait_row(self, ident, sinks=("elasticsearch", "redis")):
        return poll(lambda: self.check_row(ident, sinks), self.args.timeout, f"row {ident} converged in {sinks}")

    def defer(self, fn):
        self.cleanups.append(fn)

    def restore(self):
        errors = []
        while self.cleanups:
            try:
                self.cleanups.pop()()
            except Exception as exc:
                errors.append(str(exc))
        self.report["cleanup_errors"].extend(errors)
        if errors:
            raise RuntimeError("Fault cleanup failed: " + "; ".join(errors))

    @contextlib.contextmanager
    def paused(self, service):
        # Register restoration before mutation, including partial command failures.
        self.defer(lambda: self.dc("unpause", service))
        self.dc("pause", service)
        try:
            yield
        finally:
            self.restore()

    def aliases(self):
        state = self.http("GET", "/_alias/events-read,events-write")
        readers = [i for i, v in state.items() if "events-read" in v["aliases"]]
        writers = [i for i, v in state.items() if "events-write" in v["aliases"]]
        require(len(readers) == 1 and readers == writers, f"split or absent aliases: {state}")
        require(state[writers[0]]["aliases"]["events-write"].get("is_write_index") is True,
                "write alias has no write index")
        require(state[readers[0]]["aliases"]["events-read"].get("filter") == {"term": {"deleted": False}},
                "read filter lost")
        return readers[0]

    def baseline(self):
        ident = self.row("baseline-created")
        self.update(ident, "baseline-updated")
        self.wait_row(ident)
        self.delete(ident)
        self.wait_row(ident)

    def sink_outage(self, sink):
        self.drain()
        before = self.offsets()[sink]
        dlq_before = {topic: self.bounds(topic) for topic in self.dlqs()}
        other = "redis" if sink == "elasticsearch" else "elasticsearch"
        with self.paused(sink):
            ident = self.row(sink + "-offline", wait=False)
            self.wait_row(ident, sinks=(other,))
            # Observe throughout the fault rather than assuming sleep implies success.
            deadline = time.monotonic() + self.args.duration
            while time.monotonic() < deadline:
                require(self.offsets()[sink] == before, "unavailable sink advanced its source offset")
                require(all(self.bounds(topic) == end for topic, end in dlq_before.items()), "outage went to DLQ")
                time.sleep(1)
            self.report["scenarios"][-1]["withheld_commit"] = {"sink": sink, "offset": before,
                                                             "healthy_sink": other, "canary_id": ident}
        self.wait_row(ident)
        self.drain()

    def broker_outage(self):
        self.drain()
        with self.paused("redpanda"):
            ident = self.row("broker-offline", wait=False)
            time.sleep(self.args.duration)
        self.wait_row(ident)
        self.drain()

    def debezium_outage(self):
        self.drain()
        before = self.offsets()
        with self.paused("debezium"):
            ident = self.row("connector-offline", wait=False)
            time.sleep(self.args.duration)
            require(self.offsets() == before, "source progressed while its only connector was paused")
        self.wait_row(ident)
        self.drain()

    def worker_crash(self):
        identifiers = []
        for iteration in range(self.args.iterations):
            # Pausing makes the backlog certain; SIGKILL then discards process state.
            for service in ("worker-elasticsearch", "worker-redis"):
                self.defer(lambda s=service: self.dc("start", s))
                self.defer(lambda s=service: self.dc("unpause", s, check=False))
                self.dc("pause", service)
            ident = self.row(f"crash-{iteration}", wait=False)
            identifiers.append(ident)
            time.sleep(self.rng.uniform(0, min(self.args.duration, 2)))
            # Unpause before kill: some Docker engines reject killing paused containers.
            self.dc("unpause", "worker-elasticsearch", "worker-redis")
            self.dc("kill", "-s", "SIGKILL", "worker-elasticsearch", "worker-redis")
            self.restore()
        for ident in identifiers:
            self.wait_row(ident)
        self.drain()

    def poison_pill(self):
        self.drain()
        starts = {topic: self.bounds(topic) for topic in self.dlqs()}
        key, value = '{"payload":{"id":0}}', "this is not JSON\x00"
        position = self.probe("produce", topic="app.public.events", key=key, value=value)["offset"]
        ident = self.row("after-poison")
        self.drain()
        for sink, topic in zip(("elasticsearch", "redis"), self.dlqs()):
            end = self.bounds(topic)
            require(end > starts[topic], "poison missing from DLQ")
            messages = self.probe("read", topic=topic, offset=starts[topic], limit=end - starts[topic])
            matches = []
            for message in messages:
                record = json.loads(base64.b64decode(message["value"]))
                if record["offset"] == position and record["topic"] == "app.public.events":
                    matches.append(record)
                    require(base64.b64decode(record["key_base64"]).decode() == key, "DLQ changed key bytes")
                    require(base64.b64decode(record["value_base64"]).decode() == value, "DLQ changed value bytes")
                    require(record["sink"] == sink and record["partition"] == 0, "DLQ source identity differs")
            require(matches, f"missing exact poison record in {topic}")
        self.wait_row(ident)

    def dlq_blocked(self):
        topic = f"chaos.{self.token}.dlqbarrier"
        group = f"chaos-{self.token}-dlqbarrier"
        name = self.project + "-dlqbarrier"
        self.create_topic(topic)
        self.probe("produce", topic=topic, key="bad-key", value="bad-value")
        self.probe("produce", topic=topic, key="ignored", value=None)  # Later tombstone must not get committed.
        container = [None]
        self.defer(lambda: self.sh(["docker", "rm", "-f", container[0]]) if container[0] else None)
        launched = self.dc("run", "-d", "--no-deps", "--name", name,
                "-e", "CDC_TOPIC=" + topic, "-e", "CONSUMER_GROUP=" + group,
                "-e", "DLQ_TOPIC=chaos." + self.token + ".missing", "worker-elasticsearch")
        container[0] = launched.stdout.strip().splitlines()[-1]
        require(re.fullmatch(r"[a-f0-9]{12,64}", container[0]), "unexpected detached container ID")
        started = time.monotonic()
        def failed_without_commit():
            offset = self.probe("offset", topic=topic, group=group)["offset"]
            require(offset in (-1, 0), "failed DLQ allowed poison/later offset commit")
            logs = self.sh(["docker", "logs", container[0]]).stdout
            return (time.monotonic() - started >= self.args.duration
                    and "DLQ failed, offset not committed" in logs)
        # An invalid offset must fail immediately, not be swallowed as a poll retry.
        deadline = started + self.args.timeout
        while not failed_without_commit():
            require(time.monotonic() < deadline, "did not observe an actual DLQ failure before deadline")
            time.sleep(1)
        self.restore()

    def version_replay(self):
        ident = self.row("version-current")
        self.drain()
        version = self.es_doc(ident)["_version"]
        for sink in ("elasticsearch", "redis"):
            for v in (version, max(1, version - 1)):
                self.probe("apply", service=sink, event={"ID": str(ident), "Version": v,
                           "Data": {"id": ident, "name": "stale-overwrite"}})
        self.check_row(ident)
        self.delete(ident)
        self.wait_row(ident)
        tombstone_version = self.es_doc(ident)["_version"]
        for sink in ("elasticsearch", "redis"):
            for v in (version, tombstone_version):
                self.probe("apply", service=sink, event={"ID": str(ident), "Version": v,
                           "Data": {"id": ident, "name": "resurrection"}})
        self.check_row(ident)

    def sink_restart(self):
        ident = self.row("durable-before-restart")
        self.drain()
        self.dc("restart", "redis", "elasticsearch", timeout=180)
        self.wait_row(ident)
        self.update(ident, "durable-after-restart")
        self.wait_row(ident)

    def alias_missing(self):
        self.drain()
        source = self.aliases()
        before = self.offsets()["elasticsearch"]
        self.defer(lambda: self.http("POST", "/_aliases", {"actions": [{"add": {
            "index": source, "alias": "events-write", "is_write_index": True}}]}))
        self.http("POST", "/_aliases", {"actions": [{"remove": {"index": source, "alias": "events-write", "must_exist": True}}]})
        ident = self.row("alias-missing", wait=False)
        self.wait_row(ident, sinks=("redis",))
        time.sleep(self.args.duration)
        require(self.offsets()["elasticsearch"] == before, "missing alias advanced ES commit")
        # GET /events-write would also resolve an alias, but here it must be absent.
        self.http("GET", "/events-write", status=(404,))
        self.restore()
        self.wait_row(ident)

    def reindex_command(self, target, *, bad=False):
        args = ["run", "--rm", "--no-deps", "reindex", "-target", target, "-timeout", "3m"]
        if bad:
            args += ["-mapping", "/config/chaos-bad-mapping.json"]
        return args

    def reindex_live(self):
        ident = self.row("reindex-live-0")
        deleted = self.row("reindex-delete")
        self.delete(deleted)
        self.wait_row(deleted)
        before_version = self.es_doc(deleted)["_version"]
        target = "events-vchaos-" + uuid.uuid4().hex[:10]
        source = self.aliases()
        errors = []
        def write_load():
            try:
                for n in range(max(5, self.args.iterations)):
                    self.update(ident, f"reindex-live-{n + 1}")
                    time.sleep(0.2)
            except Exception as exc:
                errors.append(str(exc))
        thread = threading.Thread(target=write_load)
        thread.start()
        argv = ["docker", "compose", "-p", self.project, "-f", str(self.config), "--profile", "tools",
                *self.reindex_command(target)]
        # Files avoid subprocess pipe deadlocks while alias snapshots are sampled.
        log_path = self.state / (target + ".log")
        try:
            with log_path.open("w") as log:
                proc = subprocess.Popen(argv, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.defer(lambda: self.stop_process(proc))
                deadline = time.monotonic() + self.args.timeout
                while proc.poll() is None:
                    require(self.aliases() in (source, target), "unexpected alias destination")
                    require(time.monotonic() < deadline, "reindex process timed out")
                    time.sleep(0.1)
                require(proc.returncode == 0, "reindex failed: " + log_path.read_text()[-4000:])
        finally:
            thread.join(timeout=self.args.timeout)
        require(not thread.is_alive() and not errors, f"write workload failed: {errors}")
        require(self.aliases() == target, "target not promoted")
        require(self.es_doc(deleted)["_version"] == before_version, "tombstone version lost in copy")
        self.wait_row(ident)
        self.wait_row(deleted)
        self.drain()
        self.restore()

    @staticmethod
    def stop_process(proc):
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def reindex_failure(self):
        self.row("not-a-number-so-long-mapping-must-fail")
        self.drain()
        source = self.aliases()
        target = "events-vchaos-fail-" + uuid.uuid4().hex[:10]
        result = self.dc(*self.reindex_command(target, bad=True), check=False, timeout=240)
        require(result.returncode != 0, "incompatible mapping unexpectedly promoted")
        lock = self.http("GET", "/cdc-reindex-locks/_doc/events")["_source"]
        require(lock.get("source") == source and lock.get("target") == target and lock.get("task"),
                "expected durable checkpoint with task identity")
        task = self.http("GET", "/_tasks/" + lock["task"])
        require(task.get("completed"), "task still running; leaving lock/fence for inspection")
        self.defer(lambda: self.recover_failed_copy(source, target, lock["task"]))
        require(self.aliases() == source, "failed copy switched aliases")
        settings = self.http("GET", f"/{source}/_settings")
        require(settings[source]["settings"]["index"]["blocks"]["write"] == "true", "failed copy lost fence")
        before = self.offsets()["elasticsearch"]
        ident = self.row("redis-during-failed-reindex", wait=False)
        self.wait_row(ident, sinks=("redis",))
        time.sleep(self.args.duration)
        require(self.offsets()["elasticsearch"] == before, "fenced worker advanced offsets")
        self.restore()
        self.wait_row(ident)
        self.drain()

    def recover_failed_copy(self, source, target, task_id):
        # Recovery is restricted to our known completed failed task in this disposable lab.
        task = self.http("GET", "/_tasks/" + task_id)
        require(task.get("completed"), "refusing recovery of an active/unknown task")
        lock = self.http("GET", "/cdc-reindex-locks/_doc/events")["_source"]
        require(lock.get("target") == target and lock.get("task") == task_id, "lock ownership changed")
        require(self.aliases() == source, "aliases moved; refusing stale-index rollback")
        self.http("PUT", f"/{source}/_settings", {"index.blocks.write": False})
        self.http("DELETE", "/" + target)
        self.http("DELETE", "/cdc-reindex-locks/_doc/events?refresh=wait_for")

    def reindex_lock(self):
        target = "events-vchaos-lock-" + self.token
        self.http("PUT", "/cdc-reindex-locks/_create/events", {"test_owner": self.token})
        def release():
            lock = self.http("GET", "/cdc-reindex-locks/_doc/events")["_source"]
            require(lock.get("test_owner") == self.token, "lock ownership changed")
            self.http("DELETE", "/cdc-reindex-locks/_doc/events")
        self.defer(release)
        source = self.aliases()
        result = self.dc(*self.reindex_command(target), check=False)
        require(result.returncode != 0, "competing coordinator acquired lock")
        require(self.aliases() == source, "lock conflict changed aliases")
        self.http("GET", "/" + target, status=(404,))
        self.restore()

    def collect(self, name):
        directory = self.state / "results" / self.token
        directory.mkdir(parents=True, exist_ok=True)
        result = self.dc("logs", "--no-color", "--tail", "150", check=False, timeout=30)
        (directory / (name + ".log")).write_text(result.stdout + result.stderr)

    def run(self, names):
        failed = True
        exit_code = 1
        try:
            self.setup()
            for name in names:
                print(f"RUN  {name}: {SCENARIOS[name]}", flush=True)
                start = time.monotonic()
                item = {"name": name, "status": "running"}
                self.report["scenarios"].append(item)
                try:
                    before = {topic: self.bounds(topic) for topic in self.dlqs()}
                    item["before"] = {"dlq_end_offsets": before, "source_commits": self.offsets(),
                                      "alias_index": self.aliases()}
                    if name.endswith("-outage") and name.split("-outage")[0] in ("redis", "elasticsearch"):
                        self.sink_outage(name.split("-outage")[0])
                    else:
                        getattr(self, name.replace("-", "_"))()
                    self.restore()
                    if name != "poison-pill":
                        require(all(self.bounds(topic) == end for topic, end in before.items()),
                                "non-poison fault generated DLQ records")
                    self.drain()
                    item["after"] = {"dlq_end_offsets": {topic: self.bounds(topic) for topic in self.dlqs()},
                                     "source_commits": self.offsets(), "alias_index": self.aliases()}
                    item["status"] = "passed"
                    print(f"PASS {name}", flush=True)
                except BaseException as exc:
                    item.update(status="interrupted" if isinstance(exc, KeyboardInterrupt) else "failed",
                                error=str(exc), traceback=traceback.format_exc())
                    print(f"FAIL {name}: {exc}", file=sys.stderr, flush=True)
                    exit_code = 130 if isinstance(exc, KeyboardInterrupt) else 1
                    try:
                        self.restore()
                    except Exception as cleanup:
                        item["cleanup_error"] = str(cleanup)
                    try:
                        self.collect(name)
                    except Exception as collection:
                        item["collection_error"] = str(collection)
                    break
                finally:
                    item["seconds"] = round(time.monotonic() - start, 3)
            else:
                failed = False
                exit_code = 0
        except BaseException as exc:
            self.report["setup_or_runner_error"] = str(exc)
            self.report["traceback"] = traceback.format_exc()
            exit_code = 130 if isinstance(exc, KeyboardInterrupt) else 1
            print(f"Runner failed: {exc}", file=sys.stderr, flush=True)
        finally:
            try:
                self.restore()
            except Exception:
                failed = True
                exit_code = 1
            # Only run-owned DB fixtures are deleted. Tombstones intentionally remain.
            if self.ids:
                try:
                    self.sql("DELETE FROM events WHERE id IN (" + ",".join(map(str, self.ids)) + ");")
                except Exception as exc:
                    self.report["cleanup_errors"].append(str(exc))
                    failed = True
                    exit_code = 1
            self.report["commands"] = self.commands
            self.report["status"] = "failed" if failed else "passed"
            self.save_report()
        return exit_code

    def save_report(self):
        directory = self.state / "results" / self.token
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "report.json"
        self.report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        path.write_text(json.dumps(self.report, indent=2))
        print(f"Report: {path}", flush=True)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("list", "plan", "run", "status", "recover", "down"))
    p.add_argument("--project", default="cdc-chaos", help="dedicated project, must start with cdc-chaos")
    p.add_argument("--scenario", action="append", choices=SCENARIOS, help="repeat to select scenarios")
    p.add_argument("--all", action="store_true")
    p.add_argument("--exclude", action="append", default=[], choices=SCENARIOS)
    p.add_argument("--duration", type=float, default=8, help="fault hold seconds, 1..120")
    p.add_argument("--timeout", type=float, default=180, help="assertion deadline, 20..900 seconds")
    p.add_argument("--iterations", type=int, default=3, help="crash iterations, 1..20")
    p.add_argument("--seed", type=int, default=20261022, help="crash scheduling seed")
    p.add_argument("--volumes", action="store_true", help="with down: erase only this lab's data volumes")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    require(1 <= args.duration <= 120 and 20 <= args.timeout <= 900 and 1 <= args.iterations <= 20,
            "duration/timeout/iterations outside supported bounds")
    names = select_scenarios(args)
    if args.command in ("list", "plan"):
        for name in (SCENARIOS if args.command == "list" else names):
            print(f"{name:24s} {SCENARIOS[name]}")
        if args.command == "plan":
            print(f"Project={args.project}, hold={args.duration}s, iterations={args.iterations}, seed={args.seed}")
        return 0
    lab = Lab(args)
    if args.command == "run":
        require(names, "no scenarios selected")
        return lab.run(names)
    require(lab.config.exists(), "lab configuration missing; run a scenario first")
    if args.command == "status":
        print(lab.dc("ps", "-a").stdout)
        return 0
    if args.command == "recover":
        # Restore process availability only; never guess how to recover migration metadata.
        lab.dc("unpause", *SERVICES, check=False)
        lab.dc("start", *SERVICES, check=False)
        print("Services resumed. Inspect reindex lock/aliases before running more scenarios.")
        return 0
    lab.dc("down", "--remove-orphans", *(["--volumes"] if args.volumes else []))
    return 0


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        raise SystemExit(main())
    except (AssertionError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
