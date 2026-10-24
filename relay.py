#!/usr/bin/env python3
"""Coalescing webhook relay for one bot routine (prototype, stdlib only).

POST /event  any JSON. Idle -> forward now and mark active. Active -> queue (202).
POST /done   {"run_id": str, "note": str?}. Run's last step. Pending -> one batched
             wake; otherwise idle. A lease timeout un-wedges runs that never call /done.
Every wake: {"events", "pending_count", "last_note", "run_id"}; pending_count is the
number of events in this batch that queued while a run was active.
Config (env): RELAY_STATE, RELAY_DOWNSTREAM_URL_FILE, RELAY_DOWNSTREAM_KEY_FILE,
RELAY_INBOUND_SECRET_FILE, RELAY_LEASE_SECONDS (900), RELAY_TICK_SECONDS (30),
RELAY_BIND (127.0.0.1), RELAY_PORT (8787), RELAY_RETRIES (5), RELAY_BACKOFF (1.0).
"""
import hmac, ipaddress, json, os, random, socket, threading, time, urllib.error, urllib.request, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEEN_MAX = 1000
MAX_BODY = 64 * 1024
TIMEOUT = 10.0


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None                    # 3xx becomes an HTTPError: never resend the key


OPENER = urllib.request.build_opener(_NoRedirect)


def _read(path):
    if not path:
        return None
    with open(path) as f:
        return f.read().strip()


class Relay:
    def __init__(self, state_path, url_file, key_file, secret_file=None,
                 lease=900.0, retries=5, backoff=1.0, timeout=TIMEOUT):
        budget = retries * timeout + sum(backoff * 2 ** i * 1.5 for i in range(retries))
        if budget >= lease:
            raise ValueError(f"retry budget {budget:.0f}s must be shorter than lease {lease:.0f}s")
        self.state_path, self.url_file, self.key_file = state_path, url_file, key_file
        self.secret = _read(secret_file)
        self.lease, self.retries, self.backoff, self.timeout = lease, retries, backoff, timeout
        self.lock = threading.Lock()
        self.st = {"active": False, "since": None, "run_id": None,
                   "pending": [], "last_note": None, "seen": [], "inflight": None,
                   "orphan_run_id": None}
        if os.path.exists(state_path):
            with open(state_path) as f:
                self.st.update(json.load(f))
            if self.st["inflight"]:    # killed mid-forward: delivery unknown, so re-send
                self.st["pending"] = self.st["inflight"]["events"] + self.st["pending"]
                self.st["inflight"] = None
                self._save()
            if self.st["active"]:      # the run we were tracking is unknowable now
                self.st["orphan_run_id"] = self.st["run_id"]   # keep its /done note
                self._idle()

    # -- state -------------------------------------------------------------
    def _save(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.st, f)
        os.replace(tmp, self.state_path)

    def _idle(self):
        self.st.update(active=False, since=None, run_id=None, inflight=None)
        self._save()

    def _expired(self):
        return self.st["active"] and time.time() - (self.st["since"] or 0) > self.lease

    def _start_run(self, events, pending_count):
        """Caller holds the lock. Marks active, persists the batch, dispatches one wake.

        The batch stays in state["inflight"] until the bot answers 2xx (or the run's
        /done arrives), so a crash mid-forward re-sends it on restart: at-least-once.
        """
        run_id = uuid.uuid4().hex[:12]
        body = {"events": events, "pending_count": pending_count,
                "last_note": self.st["last_note"], "run_id": run_id}
        self.st.update(active=True, since=time.time(), run_id=run_id, pending=[],
                       inflight=body)
        self._save()
        threading.Thread(target=self._forward, args=(body,), daemon=True).start()

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
            queued = self.st["pending"]
            self._start_run(queued + [ev], len(queued))
            return 200, {"status": "forwarded"}

    def done(self, body):
        rid = body.get("run_id")
        if not rid:
            return 400, {"error": "run_id required"}
        with self.lock:
            if rid != self.st["run_id"]:
                if rid == self.st["orphan_run_id"] and "note" in body:
                    # run from before a relay restart: keep its note for the next wake
                    self.st.update(last_note=body["note"], orphan_run_id=None)
                    self._save()
                    return 200, {"status": "note kept (run predates restart)"}
                return 409, {"status": "stale run_id"}
            self.st["inflight"] = None         # the run exists, so its wake arrived
            if "note" in body:
                self.st["last_note"] = body["note"]
            if self.st["pending"]:
                batch = self.st["pending"]
                self._start_run(batch, len(batch))
                return 200, {"status": "forwarded batch", "count": len(batch)}
            self._idle()
            return 200, {"status": "idle"}

    def tick(self):
        """Periodic: expire dead runs and re-forward events stranded while idle."""
        with self.lock:
            if self._expired():
                self._idle()
            if not self.st["active"] and self.st["pending"]:
                self._start_run(self.st["pending"], len(self.st["pending"]))

    def _forward(self, body):
        data = json.dumps(body).encode()
        for attempt in range(self.retries):
            try:
                req = urllib.request.Request(
                    _read(self.url_file), data=data, method="POST",
                    headers={"Content-Type": "application/json",
                             "Authorization": "Bearer " + _read(self.key_file)})
                with OPENER.open(req, timeout=self.timeout) as r:
                    if 200 <= r.status < 300:
                        self._delivered(body["run_id"])
                        return True
                    err = f"status {r.status}"
            except urllib.error.HTTPError as e:  # never log URL or key
                err = f"status {e.code}"
            except Exception as e:
                err = type(e).__name__
            print(f"relay: forward attempt {attempt + 1} failed ({err})", flush=True)
            time.sleep(self.backoff * 2 ** attempt * random.uniform(0.5, 1.5))
        with self.lock:                        # give up: requeue, go idle; tick retries
            if self.st["run_id"] == body["run_id"]:
                self.st["pending"] = body["events"] + self.st["pending"]
                self._idle()
        return False

    def _delivered(self, run_id):
        with self.lock:
            inf = self.st["inflight"]
            if inf and inf["run_id"] == run_id:
                self.st["inflight"] = None
                self._save()

    def authorized(self, headers):
        if not self.secret:
            return True
        got = headers.get("Authorization", "").removeprefix("Bearer ")
        return hmac.compare_digest(got.encode(), self.secret.encode())


def _loopback(host):
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class _Server6(ThreadingHTTPServer):
    address_family = socket.AF_INET6


def make_server(relay, bind="127.0.0.1", port=8787):
    if not _loopback(bind) and not relay.secret:
        raise ValueError("refusing a non-loopback bind without RELAY_INBOUND_SECRET_FILE")

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, obj):
            out = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out)))
            if code == 413:
                self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(out)

        def do_POST(self):
            if not relay.authorized(self.headers):
                return self._send(401, {"error": "unauthorized"})
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                n = -1
            if n < 0 or n > MAX_BODY:
                self.close_connection = True
                return self._send(413, {"error": "body too large"})
            try:
                body = json.loads(self.rfile.read(n) or b"{}")
            except ValueError:
                return self._send(400, {"error": "bad json"})
            if self.path == "/event":
                return self._send(*relay.event(body))
            if self.path == "/done":
                return self._send(*relay.done(body if isinstance(body, dict) else {}))
            self._send(404, {"error": "not found"})

    return (_Server6 if ":" in bind else ThreadingHTTPServer)((bind, port), H)


def main():
    e = os.environ
    relay = Relay(e.get("RELAY_STATE", "relay-state.json"),
                  e["RELAY_DOWNSTREAM_URL_FILE"], e["RELAY_DOWNSTREAM_KEY_FILE"],
                  e.get("RELAY_INBOUND_SECRET_FILE"),
                  float(e.get("RELAY_LEASE_SECONDS", 900)),
                  int(e.get("RELAY_RETRIES", 5)), float(e.get("RELAY_BACKOFF", 1.0)))
    tick = float(e.get("RELAY_TICK_SECONDS", 30))
    srv = make_server(relay, e.get("RELAY_BIND", "127.0.0.1"), int(e.get("RELAY_PORT", 8787)))

    def ticker():
        while True:
            relay.tick()                       # first tick flushes anything left at startup
            time.sleep(tick)
    threading.Thread(target=ticker, daemon=True).start()
    print(f"relay: listening on {srv.server_address[0]}:{srv.server_address[1]}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
