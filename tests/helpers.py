import json, os, sys, tempfile, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request, urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class FakeBot:
    """Fake downstream bot webhook on localhost.

    received/auth: accepted (200) POSTs. requests: every request of any method, so a
    followed redirect (which urllib turns into a GET) is still seen.

    script: status codes (or (code, body bytes)) to answer first (then 200). A 3xx redirects to `redirect_to`.
    """

    def __init__(self, fail_first=0, script=None, redirect_to=None):
        self.received, self.auth, self.hits = [], [], 0
        self.requests = []             # every request, any method: (method, auth, body)
        self.script = list(script or []) + [503] * fail_first
        bot = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _any(self):
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b""
                body = json.loads(raw) if raw else None
                bot.hits += 1
                bot.requests.append((self.command, self.headers.get("Authorization"), body))
                code = bot.script.pop(0) if bot.script else 200
                code, reply = code if isinstance(code, tuple) else (code, b"")
                if code == 200 and self.command == "POST":
                    bot.received.append(body)
                    bot.auth.append(self.headers.get("Authorization"))
                self.send_response(code)
                if 300 <= code < 400:
                    self.send_header("Location", redirect_to)
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

            do_POST = do_GET = do_PUT = do_HEAD = _any

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
