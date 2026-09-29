"""The webhook receiver's formatting. The HTTP server itself is a few lines of stdlib."""
from scripts.alert_receiver import describe


def _payload():
    return {"alerts": [
        {"status": "firing", "labels": {"alertname": "WorkerDown", "severity": "page"},
         "annotations": {"summary": "The worker is not being scraped"}},
        {"status": "resolved", "labels": {"alertname": "ApiLatencySlowBurn", "severity": "page"},
         "annotations": {}},
    ]}


def test_one_line_per_alert_with_status_name_and_severity():
    lines = describe(_payload())

    assert len(lines) == 2
    assert lines[0].startswith("FIRING")
    assert "WorkerDown [page] The worker is not being scraped" in lines[0]
    assert lines[1].startswith("RESOLVED")


def test_a_payload_with_no_alerts_gives_nothing():
    assert describe({}) == []
    assert describe({"alerts": []}) == []


def test_missing_fields_do_not_raise():
    assert describe({"alerts": [{}]}) == ["?        ? [?] "]
