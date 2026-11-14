"""A local HTTP framing probe for BOOK.md, not the Pocket CI build engine."""

import argparse
import json
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, status, value):
        body = json.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        target = urlsplit(self.path)
        if target.path == "/snapshot":
            self.send_json(HTTPStatus.OK, {"lesson": "one complete JSON response"})
            return
        if target.path != "/logs":
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "unknown lab endpoint"})
            return

        query = parse_qs(target.query, keep_blank_values=True)
        try:
            after_values = query.get("after", ["0"])
            fail_values = query.get("fail", ["0"])
            if len(after_values) != 1 or len(fail_values) != 1:
                raise ValueError("parameters must occur once")
            after_text = after_values[0]
            if not after_text.isascii() or not after_text.isdecimal():
                raise ValueError("after must be an integer from 0 to 6")
            after = int(after_text)
            if not 0 <= after <= 6 or fail_values[0] not in ("0", "1"):
                raise ValueError("after must be 0..6 and fail must be 0 or 1")
        except ValueError as error:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Cache-Control", "no-store")
        # This low-level lab writes chunks itself; axum handles this for Rust.
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        try:
            for seq in range(after + 1, 7):
                self.write_record(
                    {"type": "log", "seq": seq, "stage": "demo", "text": f"Line {seq}"}
                )
                time.sleep(1)
            self.write_record(
                {
                    "type": "end",
                    "state": "failed" if fail_values[0] == "1" else "succeeded",
                    "last_seq": 6,
                }
            )
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as error:
            self.log_error("log observer disconnected: %s", error)
            self.close_connection = True

    def write_record(self, frame):
        payload = (json.dumps(frame, separators=(",", ":")) + "\n").encode("utf-8")
        self.wfile.write(f"{len(payload):X}\r\n".encode("ascii"))
        self.wfile.write(payload)
        self.wfile.write(b"\r\n")
        self.wfile.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=3000)
    args = parser.parse_args()
    with ThreadingHTTPServer(("127.0.0.1", args.port), Handler) as server:
        print(f"HTTP/1.1 lab: http://127.0.0.1:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping HTTP lab.", flush=True)


if __name__ == "__main__":
    main()
