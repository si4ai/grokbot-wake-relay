import json, os, sys, tempfile, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request, urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class FakeBot:
    """Fake downstream bot webhook on localhost that records every POST."""

    def __init__(self, fail_first=0):
        self.received, self.auth, self.fail_left = [], [], fail_first
        bot = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if bot.fail_left > 0:
                    bot.fail_left -= 1
                    self.send_response(503)
                else:
                    bot.received.append(body)
                    bot.auth.append(self.headers.get("Authorization"))
                    self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}/hook"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def wait_for(self, n, timeout=5.0):
        end = time.time() + timeout
        while time.time() < end and len(self.received) < n:
            time.sleep(0.02)
        return len(self.received) >= n

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


def write(dirpath, name, text):
    p = os.path.join(dirpath, name)
    with open(p, "w") as f:
        f.write(text)
    os.chmod(p, 0o600)
    return p


def post(url, body, secret=None):
    headers = {"Content-Type": "application/json"}
    if secret:
        headers["Authorization"] = "Bearer " + secret
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers,
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")
