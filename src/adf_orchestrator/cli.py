"""Offline validator and diagnostics; no orchestration engine."""
import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from adf_orchestrator.validation import validate_graph

class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            status, body = 200, {"service": "adf-orchestrator", "status": "alive", "implementation": "scaffold"}
        elif self.path == "/readyz":
            status, body = 503, {"code": "SCAFFOLD_NOT_IMPLEMENTED"}
        else:
            status, body = 404, {"code": "NOT_FOUND"}
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("validate")
    check.add_argument("path", type=Path)
    serve = sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()
    if args.command == "validate":
        try:
            errors = validate_graph(json.loads(args.path.read_text(encoding="utf-8")))
        except (OSError, ValueError) as exc:
            errors = [str(exc)]
        print(json.dumps({"valid": not errors, "errors": errors, "scope": "basic structural validation only"}))
        raise SystemExit(bool(errors))
    HTTPServer((args.host, args.port), Handler).serve_forever()

if __name__ == "__main__":
    main()
