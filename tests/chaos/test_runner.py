"""Host-side harness checks; no Docker required. Run with unittest discovery."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("chaos_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class HarnessTests(unittest.TestCase):
    def run_lab(self, lab, names):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return lab.run(names)

    def args(self, *extra):
        return runner.parser().parse_args(["run", *extra])

    def lab(self):
        lab = runner.Lab(self.args())
        lab.setup = Mock()
        lab.bounds = Mock(return_value=0)
        lab.offsets = Mock(return_value={"redis": 0, "elasticsearch": 0})
        lab.aliases = Mock(return_value="events-v1")
        lab.drain = Mock()
        lab.collect = Mock()
        lab.save_report = Mock()
        return lab

    def test_selection_and_exclusions(self):
        args = self.args("--scenario", "baseline", "--scenario", "baseline", "--scenario", "redis-outage")
        self.assertEqual(runner.select_scenarios(args), ["baseline", "redis-outage"])
        args = self.args("--all", "--exclude", "broker-outage")
        selected = runner.select_scenarios(args)
        self.assertEqual(len(selected), len(runner.SCENARIOS) - 1)
        self.assertNotIn("broker-outage", selected)

    def test_list_and_plan_never_call_docker(self):
        with patch.object(subprocess, "run", side_effect=AssertionError("Docker invoked")):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(runner.main(["list"]), 0)
                self.assertEqual(runner.main(["plan", "--all"]), 0)

    def test_refuses_non_lab_project(self):
        for name in ["distributed-cdc", "production", "cdc-chaos/../prod", "cdc-chaos;echo"]:
            with self.assertRaises(AssertionError):
                runner.Lab(self.args("--project", name))

    def test_failed_scenario_stops_following_faults_and_reports_failure(self):
        lab = self.lab()
        restored = []
        def fail():
            lab.defer(lambda: restored.append(True))
            raise AssertionError("lost update")
        lab.baseline = fail
        lab.poison_pill = Mock()
        self.assertEqual(self.run_lab(lab, ["baseline", "poison-pill"]), 1)
        self.assertEqual(restored, [True])
        lab.poison_pill.assert_not_called()
        self.assertEqual(lab.report["scenarios"][0]["status"], "failed")
        self.assertEqual(lab.report["status"], "failed")
        lab.save_report.assert_called_once()

    def test_interrupted_scenario_restores_fault_and_is_not_a_pass(self):
        lab = self.lab()
        restored = []
        def interrupt():
            lab.defer(lambda: restored.append(True))
            raise KeyboardInterrupt("cancelled")
        lab.baseline = interrupt
        self.assertEqual(self.run_lab(lab, ["baseline"]), 130)
        self.assertEqual(restored, [True])
        self.assertEqual(lab.report["scenarios"][0]["status"], "interrupted")
        self.assertEqual(lab.report["status"], "failed")

    def test_setup_failure_still_produces_failure_report(self):
        lab = self.lab()
        lab.setup.side_effect = RuntimeError("not enough memory")
        self.assertEqual(self.run_lab(lab, ["baseline"]), 1)
        self.assertIn("not enough memory", lab.report["setup_or_runner_error"])
        lab.save_report.assert_called_once()

    def test_cleanup_is_lifo_and_attempts_every_restoration(self):
        lab = self.lab()
        restored = []
        lab.defer(lambda: restored.append("first"))
        def failed_restore():
            restored.append("second")
            raise RuntimeError("cannot unpause")
        lab.defer(failed_restore)
        with self.assertRaises(RuntimeError):
            lab.restore()
        self.assertEqual(restored, ["second", "first"])
        self.assertTrue(lab.report["cleanup_errors"])
        self.assertEqual(lab.cleanups, [])

    def test_cleanup_failure_cannot_report_success(self):
        lab = self.lab()
        lab.baseline = lambda: lab.defer(lambda: (_ for _ in ()).throw(RuntimeError("failed restoration")))
        self.assertEqual(self.run_lab(lab, ["baseline"]), 1)
        self.assertEqual(lab.report["status"], "failed")

    def test_success_records_observed_offsets(self):
        lab = self.lab()
        lab.baseline = Mock()
        self.assertEqual(self.run_lab(lab, ["baseline"]), 0)
        item = lab.report["scenarios"][0]
        self.assertEqual(item["status"], "passed")
        self.assertEqual(item["after"]["source_commits"], {"redis": 0, "elasticsearch": 0})

    def test_config_strips_ports_and_keeps_project_owned_volumes(self):
        lab = self.lab()
        config = {"name": "cdc-chaos", "services": {
            "redis": {"ports": [{"published": "6379"}]}, "reindex": {"volumes": []}},
            "volumes": {"redis_data": {"name": "cdc-chaos_redis_data"}},
            "networks": {"default": {"name": "cdc-chaos_default"}}}
        lab.sh = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps(config), ""))
        with tempfile.TemporaryDirectory() as temporary:
            lab.state = Path(temporary)
            lab.config = lab.state / "compose.json"
            lab.prepare_config()
            actual = json.loads(lab.config.read_text())
            self.assertNotIn("ports", actual["services"]["redis"])
            self.assertEqual(actual["volumes"]["redis_data"]["name"], "cdc-chaos_redis_data")
            self.assertIn("probe", actual["services"])
            config["volumes"]["redis_data"]["name"] = "production_data"
            lab.sh.return_value.stdout = json.dumps(config)
            with self.assertRaises(AssertionError):
                lab.prepare_config()


if __name__ == "__main__":
    unittest.main()
