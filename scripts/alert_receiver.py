"""A tiny receiver for Alertmanager webhooks, for local development.

Alertmanager posts JSON to it and it prints one line per alert. It exists so the
alerting path can be tried end to end without a paging service:

    python scripts/alert_receiver.py          # listens on port 9094

observability/alertmanager.yml points at http://host.docker.internal:9094/alerts.
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


def describe(payload: dict) -> list[str]:
    """One readable line per alert in an Alertmanager webhook payload."""
    lines = []
    for alert in payload.get("alerts", []):
        labels = alert.get("labels", {})
        summary = alert.get("annotations", {}).get("summary", "")
        lines.append(
            f"{alert.get('status', '?').upper():8} {labels.get('alertname', '?')} "
            f"[{labels.get('severity', '?')}] {summary}"
        )
    return lines


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        try:
            payload = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            return
        for line in describe(payload):
            print(line, flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9094
    print(f"listening for Alertmanager webhooks on :{port}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
