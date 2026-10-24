#!/usr/bin/env python3
"""Coalescing webhook relay for one bot routine (stdlib only).

POST /event  any JSON. Idle -> forward now and mark active. Active -> queue (202).
POST /done   {"note": str, "run_id": str?}. Run's last step. Pending -> one batched
             wake; otherwise idle. A lease timeout un-wedges runs that never call /done.
Config (env): RELAY_STATE, RELAY_DOWNSTREAM_URL_FILE, RELAY_DOWNSTREAM_KEY_FILE,
RELAY_INBOUND_SECRET_FILE (optional), RELAY_LEASE_SECONDS (900), RELAY_TICK_SECONDS (30),
RELAY_BIND (127.0.0.1), RELAY_PORT (8787), RELAY_RETRIES (5), RELAY_BACKOFF (1.0).
"""
import hmac, json, os, random, threading, time, urllib.request, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEEN_MAX = 1000


def _read(path):
    if not path:
        return None
    with open(path) as f:
        return f.read().strip()


class Relay:
    def __init__(self, state_path, url_file, key_file, secret_file=None,
                 lease=900.0, retries=5, backoff=1.0):
        self.state_path, self.url_file, self.key_file = state_path, url_file, key_file
        self.secret = _read(secret_file)
        self.lease, self.retries, self.backoff = lease, retries, backoff
        self.lock = threading.Lock()
        self.st = {"active": False, "since": None, "run_id": None,
                   "pending": [], "last_note": None, "seen": []}
        if os.path.exists(state_path):
            with open(state_path) as f:
                self.st.update(json.load(f))

    # -- state -------------------------------------------------------------
    def _save(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.st, f)
        os.replace(tmp, self.state_path)

    def _expired(self):
        return self.st["active"] and time.time() - (self.st["since"] or 0) > self.lease

    def _start_run(self, events, pending_count):
        """Caller holds the lock. Marks active and dispatches one wake."""
        run_id = uuid.uuid4().hex[:12]
        self.st.update(active=True, since=time.time(), run_id=run_id, pending=[])
        self._save()
        body = {"events": events, "pending_count": pending_count,
                "last_note": self.st["last_note"], "run_id": run_id}
        threading.Thread(target=self._forward, args=(body,), daemon=True).start()
        return run_id

    # -- operations ----------------------------------------------------------
    def event(self, ev):
        eid = ev.get("id") if isinstance(ev, dict) else None
        with self.lock:
            if eid is not None:
                if eid in self.st["seen"]:
                    return 200, {"status": "duplicate"}
                self.st["seen"] = (self.st["seen"] + [eid])[-SEEN_MAX:]
            if self._expired():               # crashed run: lease lapsed
                self.st["active"] = False
            if self.st["active"]:
                self.st["pending"].append(ev)
                self._save()
                return 202, {"status": "queued", "pending": len(self.st["pending"])}
            batch = self.st["pending"] + [ev]
            self._start_run(batch, len(batch) - 1)
            return 200, {"status": "forwarded"}

    def done(self, body):
        with self.lock:
            rid = body.get("run_id")
            if rid and rid != self.st["run_id"]:
                return 409, {"status": "stale run_id"}
            if "note" in body:
                self.st["last_note"] = body["note"]
            if self.st["pending"]:
                batch = self.st["pending"]
                self._start_run(batch, len(batch))
                return 200, {"status": "forwarded batch", "count": len(batch)}
            self.st.update(active=False, since=None, run_id=None)
            self._save()
            return 200, {"status": "idle"}

    def tick(self):
        with self.lock:
            if not self._expired():
                return
            if self.st["pending"]:
                self._start_run(self.st["pending"], len(self.st["pending"]))
            else:
                self.st.update(active=False, since=None, run_id=None)
                self._save()

    def _forward(self, body):
        data = json.dumps(body).encode()
        for attempt in range(self.retries):
            try:
                req = urllib.request.Request(
                    _read(self.url_file), data=data, method="POST",
                    headers={"Content-Type": "application/json",
                             "Authorization": "Bearer " + _read(self.key_file)})
                with urllib.request.urlopen(req, timeout=10) as r:
                    if 200 <= r.status < 300:
                        return True
            except Exception as e:            # never log URL or key
                err = type(e).__name__
            else:
                err = "status"
            delay = self.backoff * 2 ** attempt * random.uniform(0.5, 1.5)
            print(f"relay: forward attempt {attempt + 1} failed ({err})", flush=True)
            time.sleep(delay)
        with self.lock:                        # give up: requeue, go idle
            if self.st["run_id"] == body["run_id"]:
                self.st["pending"] = body["events"] + self.st["pending"]
                self.st.update(active=False, since=None, run_id=None)
                self._save()
        return False

    def authorized(self, headers):
        if not self.secret:
            return True
        got = headers.get("Authorization", "").removeprefix("Bearer ")
        return hmac.compare_digest(got.encode(), self.secret.encode())


def make_server(relay, bind="127.0.0.1", port=8787):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, obj):
            out = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)

        def do_POST(self):
            if not relay.authorized(self.headers):
                return self._send(401, {"error": "unauthorized"})
            n = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(n) or b"{}")
            except ValueError:
                return self._send(400, {"error": "bad json"})
            if self.path == "/event":
                return self._send(*relay.event(body))
            if self.path == "/done":
                return self._send(*relay.done(body if isinstance(body, dict) else {}))
            self._send(404, {"error": "not found"})

    return ThreadingHTTPServer((bind, port), H)


def main():
    e = os.environ
    relay = Relay(e.get("RELAY_STATE", "relay-state.json"),
                  e["RELAY_DOWNSTREAM_URL_FILE"], e["RELAY_DOWNSTREAM_KEY_FILE"],
                  e.get("RELAY_INBOUND_SECRET_FILE"),
                  float(e.get("RELAY_LEASE_SECONDS", 900)),
                  int(e.get("RELAY_RETRIES", 5)), float(e.get("RELAY_BACKOFF", 1.0)))
    tick = float(e.get("RELAY_TICK_SECONDS", 30))

    def ticker():
        while True:
            time.sleep(tick)
            relay.tick()
    threading.Thread(target=ticker, daemon=True).start()
    srv = make_server(relay, e.get("RELAY_BIND", "127.0.0.1"), int(e.get("RELAY_PORT", 8787)))
    print(f"relay: listening on {srv.server_address[0]}:{srv.server_address[1]}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
