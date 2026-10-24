#!/usr/bin/env python3
"""Run a command detached; when it exits, POST a job_done event (stdlib only).

usage: notify_on_exit.py --job NAME --log PATH [--foreground] -- CMD [ARGS...]

Sends {"kind": "job_done", "id", "job", "exit_code", "log_path"} -- never log contents.
A signal-killed command reports 128+signum (TERM -> 143), including when the
wrapper itself gets SIGTERM/SIGINT/SIGHUP (it forwards the signal, then reports).
Target (env): NOTIFY_URL_FILE (file holding the relay /event URL or a bot webhook URL)
and optional NOTIFY_KEY_FILE (bearer key). Neither value is ever printed.
Gaps: POSIX only; if the wrapper itself is SIGKILLed, nothing is sent.
"""
import argparse, json, os, random, signal, subprocess, sys, time, urllib.error, urllib.request, uuid


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None                    # 3xx is a failure: never resend the key


OPENER = urllib.request.build_opener(_NoRedirect)


def _read(path):
    with open(path) as f:
        return f.read().strip()


def notify(payload, retries=4, backoff=1.0):
    url_file = os.environ.get("NOTIFY_URL_FILE")
    if not url_file:
        print("notify_on_exit: NOTIFY_URL_FILE not set; not notifying", file=sys.stderr)
        return False
    headers = {"Content-Type": "application/json"}
    if os.environ.get("NOTIFY_KEY_FILE"):
        headers["Authorization"] = "Bearer " + _read(os.environ["NOTIFY_KEY_FILE"])
    data = json.dumps(payload).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(_read(url_file), data=data, headers=headers,
                                         method="POST")
            with OPENER.open(req, timeout=10) as r:
                if 200 <= r.status < 300:
                    return True
        except urllib.error.HTTPError as e:
            print(f"notify_on_exit: attempt {attempt + 1} got status {e.code}", file=sys.stderr)
            if 400 <= e.code < 500:    # our request is wrong; retrying won't help
                return False
        except Exception as e:
            print(f"notify_on_exit: attempt {attempt + 1} failed ({type(e).__name__})",
                  file=sys.stderr)
        time.sleep(backoff * 2 ** attempt * random.uniform(0.5, 1.5))
    return False


def run(job, log_path, cmd):
    child, early = None, []

    def forward(signum, _frame):
        if child is None:              # signal arrived before the child existed
            early.append(signum)
            return
        try:
            os.killpg(child.pid, signum)
        except ProcessLookupError:
            pass
    for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(s, forward)      # installed before the child starts

    with open(log_path, "ab") as log:
        child = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, start_new_session=True)
    for signum in early:
        forward(signum, None)

    rc = child.wait()
    if rc < 0:                     # killed by a signal: shell convention 128+N
        rc = 128 - rc
    payload = {"kind": "job_done", "id": f"job-{job}-{uuid.uuid4().hex[:12]}",
               "job": job, "exit_code": rc, "log_path": os.path.abspath(log_path)}
    notify(payload, backoff=float(os.environ.get("NOTIFY_BACKOFF", 1.0)))
    return rc


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--job", required=True)
    p.add_argument("--log", required=True)
    p.add_argument("--foreground", action="store_true",
                   help="stay attached (default: detach and print the wrapper pid)")
    p.add_argument("cmd", nargs=argparse.REMAINDER)
    a = p.parse_args(argv)
    cmd = a.cmd[1:] if a.cmd[:1] == ["--"] else a.cmd
    if not cmd:
        p.error("missing command after --")
    if a.foreground:
        return run(a.job, a.log, cmd)
    # Detach: re-exec ourselves in a new session so the caller's run can end.
    me = [sys.executable, os.path.abspath(__file__), "--job", a.job, "--log", a.log,
          "--foreground", "--", *cmd]
    proc = subprocess.Popen(me, start_new_session=True, stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(proc.pid)
    return 0


if __name__ == "__main__":
    sys.exit(main())
