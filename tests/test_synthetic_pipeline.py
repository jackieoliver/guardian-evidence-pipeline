"""Offline provenance and deterministic-rebuild checks; all records are fabricated."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops"))
from guardian_pipeline_db import connect_db, init_db, event_record, upsert_event
from guardian_source_priority import choose_primary_source_type


class SyntheticPipelineTests(unittest.TestCase):
    def test_episode_rebuild_is_idempotent_and_retains_event_links(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic.db"
            db = connect_db(path)
            init_db(db)
            for index, (start, end) in enumerate([("12:00:00", "12:03:00"), ("12:03:10", "12:05:00")]):
                upsert_event(db, event_record({
                    "id": f"synthetic-{index}", "user_id": "example-user", "device_id": "example-device",
                    "event_type": "activity_context", "start_at": f"2026-01-01T{start}Z",
                    "end_at": f"2026-01-01T{end}Z", "source_priority": 1, "confidence": 0.9,
                    "primary_source_type": "app_metadata", "created_by": "synthetic-test",
                    "created_at": "2026-01-01T12:06:00Z",
                    "payload": {"activity_type": "coding", "app_name": "Example editor", "summary": "Synthetic editing activity"},
                }))
            args = [sys.executable, str(ROOT / "ops/guardian-pipeline-build-episodes.py"),
                    "--db", str(path), "--user-id", "example-user", "--day", "2026-01-01"]
            subprocess.run(args, check=True, capture_output=True)
            first = [tuple(row) for row in db.execute("SELECT id, started_at, ended_at FROM episode ORDER BY id")]
            self.assertEqual(len(first), 1)
            links = {row[0] for row in db.execute("SELECT event_id FROM episode_event")}
            self.assertEqual(links, {"synthetic-0", "synthetic-1"})
            subprocess.run(args, check=True, capture_output=True)
            self.assertEqual(first, [tuple(row) for row in db.execute("SELECT id, started_at, ended_at FROM episode ORDER BY id")])
            self.assertEqual(db.execute("SELECT count(*) FROM episode_event").fetchone()[0], 2)
            db.close()

    def test_direct_observation_outranks_model_interpretation(self):
        self.assertEqual(choose_primary_source_type(["codex_interpretation", "camera_visual", "app_metadata"]), "app_metadata")


if __name__ == "__main__":
    unittest.main()
