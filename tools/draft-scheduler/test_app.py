import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("POLL_PATH", "/tmp/draft-scheduler-test-unused.json")

from app import app  # noqa: E402 — imported after env


class DraftPollTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["POLL_PATH"] = str(Path(self.tmp.name) / "poll.json")
        self.client = app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def test_empty_poll(self):
        res = self.client.get("/api/poll")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["people"], [])
        self.assertEqual(data["time_label"], "4:00 PM Pacific")
        self.assertGreaterEqual(len(data["months"]), 2)
        self.assertTrue(
            any(d["enabled"] for m in data["months"] for w in m["weeks"] for d in w)
        )

    def test_page_renders(self):
        res = self.client.get("/")
        self.assertEqual(res.status_code, 200)
        self.assertIn(b"Draft Night", res.data)
        self.assertIn(b"4:00 PM Pacific", res.data)
        self.assertIn(b"Tap your name to start.", res.data)
        self.assertNotIn(b"includes names and nights", res.data)
        self.assertIn(b"nights stay saved here", res.data)
        self.assertEqual(res.headers.get("X-Robots-Tag"), "noindex, nofollow")

    def test_canonical_url_redirects(self):
        import app as draft_app

        old = draft_app.CANONICAL_URL
        draft_app.CANONICAL_URL = "https://draft-night-live.onrender.com"
        try:
            res = self.client.get("/")
            self.assertEqual(res.status_code, 302)
            self.assertEqual(res.headers.get("Location"), "https://draft-night-live.onrender.com")
        finally:
            draft_app.CANONICAL_URL = old

    def test_save_and_toggle_dates(self):
        slash = self.client.post(
            "/api/poll/",
            json={"name": "Alex", "dates": ["2026-08-22"]},
        )
        self.assertEqual(slash.status_code, 200)
        res = self.client.post(
            "/api/poll",
            json={"name": "Alex", "dates": ["2026-08-22", "2026-08-29"]},
        )
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(len(data["people"]), 1)
        self.assertEqual(data["people"][0]["dates"], ["2026-08-22", "2026-08-29"])
        self.assertEqual(data["by_date"]["2026-08-22"], ["Alex"])
        # Both Saturdays are tied; earlier date wins so the league can draft sooner.
        self.assertEqual(data["best"][0]["date"], "2026-08-22")

        res = self.client.post(
            "/api/availability",
            json={"name": "alex", "dates": ["2026-08-29"]},
        )
        data = res.get_json()
        self.assertEqual(len(data["people"]), 1)
        self.assertEqual(data["people"][0]["name"], "alex")
        self.assertEqual(data["people"][0]["dates"], ["2026-08-29"])

    def test_weekend_wins_tie(self):
        self.client.post(
            "/api/availability",
            json={"name": "Sam", "dates": ["2026-08-19", "2026-08-22"]},
        )
        data = self.client.get("/api/poll").get_json()
        # Wednesday Aug 19 and Saturday Aug 22 both have 1 person; Sat wins.
        self.assertEqual(data["best"][0]["date"], "2026-08-22")

    def test_first_game_is_marked_and_not_pickable(self):
        data = self.client.get("/api/poll").get_json()
        self.assertEqual(data["end"], "2026-09-08")
        self.assertEqual(data["kickoff"], "2026-09-09")
        days = [d for m in data["months"] for w in m["weeks"] for d in w]
        eighth = next(d for d in days if d["date"] == "2026-09-08")
        ninth = next(d for d in days if d["date"] == "2026-09-09")
        tenth = next(d for d in days if d["date"] == "2026-09-10")
        self.assertTrue(eighth["enabled"])
        self.assertFalse(ninth["enabled"])
        self.assertTrue(ninth["is_kickoff"])
        self.assertFalse(tenth["enabled"])
        self.assertEqual(
            self.client.post(
                "/api/poll", json={"name": "Alex", "dates": ["2026-09-09"]}
            ).status_code,
            400,
        )

    def test_restore_merges_without_wiping(self):
        self.client.post("/api/poll", json={"name": "Cameron", "dates": ["2026-08-22"]})
        res = self.client.post(
            "/api/restore",
            json={
                "people": [
                    {"name": "Cameron", "dates": ["2026-08-29"]},
                    {"name": "Sam", "dates": ["2026-08-30"]},
                ]
            },
        )
        people = {p["name"]: p["dates"] for p in res.get_json()["people"]}
        self.assertEqual(people["Cameron"], ["2026-08-22", "2026-08-29"])
        self.assertEqual(people["Sam"], ["2026-08-30"])

    def test_remove_person(self):
        self.client.post("/api/poll", json={"name": "Cameron", "dates": ["2026-08-22"]})
        self.client.post("/api/poll", json={"name": "Sam", "dates": ["2026-08-29"]})
        res = self.client.post("/api/remove", json={"name": "sam"})
        people = res.get_json()["people"]
        self.assertEqual([p["name"] for p in people], ["Cameron"])

    def test_two_people_keep_their_dates(self):
        self.client.post(
            "/api/poll",
            json={"name": "Cameron", "dates": ["2026-08-22"]},
        )
        res = self.client.post(
            "/api/poll",
            json={
                "name": "Sam",
                "previous_name": "Cameron",
                "dates": ["2026-08-29"],
            },
        )
        people = {p["name"]: p["dates"] for p in res.get_json()["people"]}
        self.assertEqual(people["Cameron"], ["2026-08-22"])
        self.assertEqual(people["Sam"], ["2026-08-29"])
        self.assertEqual(res.get_json()["by_date"]["2026-08-22"], ["Cameron"])
        self.assertEqual(res.get_json()["by_date"]["2026-08-29"], ["Sam"])

    def test_rejects_bad_input(self):
        self.assertEqual(
            self.client.post("/api/availability", json={"name": "", "dates": []}).status_code,
            400,
        )
        self.assertEqual(
            self.client.post(
                "/api/availability", json={"name": "Alex", "dates": ["2025-01-01"]}
            ).status_code,
            400,
        )
        self.assertEqual(
            self.client.post(
                "/api/availability", json={"name": "Alex", "dates": "2026-08-22"}
            ).status_code,
            400,
        )

    def test_strips_junk_from_name(self):
        res = self.client.post(
            "/api/availability",
            json={"name": "  <Alex!>", "dates": []},
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json()["people"][0]["name"], "Alex")


if __name__ == "__main__":
    unittest.main()
