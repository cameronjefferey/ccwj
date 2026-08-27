"""CSV export guides on /upload: Schwab is ready; others request via feedback."""

from app.marketing import compose_feedback_body
from app.upload import CSV_EXPORT_BROKERS


def test_only_schwab_csv_export_is_ready():
    ready = [b["slug"] for b in CSV_EXPORT_BROKERS if b["ready"]]
    assert ready == ["schwab"]
    slugs = [b["slug"] for b in CSV_EXPORT_BROKERS]
    for slug in ("fidelity", "vanguard", "robinhood", "interactive", "alpaca", "wealthsimple"):
        assert slug in slugs
    assert all(b["name"] for b in CSV_EXPORT_BROKERS)


def test_compose_feedback_passthrough_without_topic():
    assert compose_feedback_body("hello") == "hello"
    assert compose_feedback_body("  ") == ""
    # Footer Send-Feedback never sends topic/broker; username must not sneak in.
    assert compose_feedback_body("a bug", username="cameron") == "a bug"


def test_compose_csv_request_without_notes():
    body = compose_feedback_body(
        "",
        topic="Request: CSV upload instructions for Fidelity",
        broker="Fidelity",
        username="testingcameron",
    )
    assert "Request: CSV upload instructions for Fidelity" in body
    assert "Broker: Fidelity" in body
    assert "HappyTrader user: testingcameron" in body
    assert "Additional notes" not in body


def test_compose_csv_request_with_notes():
    body = compose_feedback_body(
        "I have a 2022 sample CSV",
        topic="Request: CSV upload instructions for Fidelity",
        broker="Fidelity",
        username="testingcameron",
    )
    assert "Additional notes:\nI have a 2022 sample CSV" in body
    assert body.startswith("Request: CSV upload instructions for Fidelity")


def test_compose_csv_request_records_sample_offer():
    yes = compose_feedback_body(
        "",
        topic="Request: CSV upload instructions for Fidelity",
        broker="Fidelity",
        offer_samples=True,
    )
    no = compose_feedback_body(
        "",
        topic="Request: CSV upload instructions for Fidelity",
        broker="Fidelity",
        offer_samples=False,
    )
    assert "Willing to provide sample CSVs: yes" in yes
    assert "Willing to provide sample CSVs: no" in no
    # Footer feedback must not grow a samples line.
    assert "sample CSVs" not in compose_feedback_body("a bug", offer_samples=True)
