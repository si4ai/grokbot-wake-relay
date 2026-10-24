import http.client, json, os, signal, socket, subprocess, sys, tempfile, threading, time, unittest
from unittest import mock
from helpers import FakeBot, write, post
import relay as R

KEY = "test-downstream-key"
SECRET = "test-inbound-secret"


class Base(unittest.TestCase):
    bot_kwargs = {}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.bot = FakeBot(**self.bot_kwargs)
        self.relay = R.Relay(os.path.join(d, "state.json"), write(d, "url", self.bot.url),
                             write(d, "key", KEY), write(d, "secret", SECRET),
                             lease=60, retries=4, backoff=0.01, timeout=5)
        self.srv = R.make_server(self.relay, "127.0.0.1", 0)
        self.port = self.srv.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def tearDown(self):
        self.srv.shutdown(); self.srv.server_close(); self.bot.close(); self.tmp.cleanup()

    def ev(self, body):
        return post(self.base + "/event", body, SECRET)

    def done(self, note=None, run_id=None):
        body = {"run_id": run_id or self.relay.st["run_id"]}
        if note is not None:
            body["note"] = note
        return post(self.base + "/done", body, SECRET)

    def expire_lease(self):
        with self.relay.lock:
            self.relay.st["since"] -= self.relay.lease + 1

    def wait_idle(self, timeout=5):
        end = time.time() + timeout
        while time.time() < end and self.relay.st["active"]:
            time.sleep(0.02)
        return not self.relay.st["active"]


class RelayTest(Base):
    def test_idle_event_forwards_immediately(self):
        self.assertEqual(self.ev({"id": "a", "x": 1})[0], 200)
        self.assertTrue(self.bot.wait_for(1))
        got = self.bot.received[0]
        self.assertEqual(got["events"], [{"id": "a", "x": 1}])
        self.assertEqual(got["pending_count"], 0)
        self.assertIsNone(got["last_note"])
        self.assertEqual(got["run_id"], self.relay.st["run_id"])
        self.assertEqual(self.bot.auth[0], "Bearer " + KEY)
        self.assertTrue(self.relay.st["active"])

    def test_burst_of_five_coalesces(self):
        codes = [self.ev({"id": f"e{i}"})[0] for i in range(5)]
        self.assertEqual(codes, [200, 202, 202, 202, 202])
        self.assertTrue(self.bot.wait_for(1))
        time.sleep(0.2)
        self.assertEqual(len(self.bot.received), 1)
        code, body = self.done("handled e0")
        self.assertEqual((code, body["count"]), (200, 4))
        self.assertTrue(self.bot.wait_for(2))
        time.sleep(0.2)
        self.assertEqual(len(self.bot.received), 2)
        batch = self.bot.received[1]
        self.assertEqual(batch["pending_count"], 4)
        self.assertEqual([e["id"] for e in batch["events"]], ["e1", "e2", "e3", "e4"])
        self.assertEqual(batch["last_note"], "handled e0")
        self.assertTrue(self.relay.st["active"])
        self.assertEqual(self.relay.st["pending"], [])

    def test_done_with_nothing_pending_goes_idle(self):
        self.ev({"id": "a"}); self.bot.wait_for(1)
        self.assertEqual(self.done("n1"), (200, {"status": "idle"}))
        self.assertFalse(self.relay.st["active"])
        self.ev({"id": "b"}); self.assertTrue(self.bot.wait_for(2))
        self.assertEqual(self.bot.received[1]["last_note"], "n1")

    def test_done_requires_run_id(self):
        self.ev({"id": "a"}); self.bot.wait_for(1)
        self.assertEqual(post(self.base + "/done", {"note": "x"}, SECRET)[0], 400)
        self.assertEqual(self.done(run_id="nope")[0], 409)
        self.assertEqual(self.done(run_id=self.bot.received[0]["run_id"])[1]["status"], "idle")

    def test_duplicate_event_id_deduped(self):
        self.ev({"id": "same"}); self.bot.wait_for(1)
        self.assertEqual(self.ev({"id": "same"})[1]["status"], "duplicate")
        self.assertEqual(self.relay.st["pending"], [])
        self.done(); time.sleep(0.1)
        self.assertEqual(len(self.bot.received), 1)

    def test_unauthorized_rejected(self):
        self.assertEqual(post(self.base + "/event", {"id": "x"}, "wrong")[0], 401)
        self.assertEqual(post(self.base + "/event", {"id": "x"})[0], 401)

    def test_oversized_body_rejected(self):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        c.request("POST", "/event", body=b'{"x":"' + b"a" * (R.MAX_BODY + 10) + b'"}',
                  headers={"Authorization": "Bearer " + SECRET,
                           "Content-Type": "application/json"})
        self.assertEqual(c.getresponse().status, 413)
        c.close()
        self.assertEqual(self.bot.hits, 0)

    def test_restart_while_active_resets_and_reforwards(self):
        self.ev({"id": "a"}); self.ev({"id": "b"}); self.bot.wait_for(1)
        end = time.time() + 5                  # restart after wake 1 was acknowledged
        while self.relay.st["inflight"] and time.time() < end:
            time.sleep(0.02)
        with self.relay.lock:                  # its save to disk has finished too
            pass
        self.assertIsNone(self.relay.st["inflight"])
        r2 = R.Relay(self.relay.state_path, self.relay.url_file, self.relay.key_file,
                     lease=60, retries=2, backoff=0.01, timeout=5)
        self.assertFalse(r2.st["active"])
        self.assertEqual([e["id"] for e in r2.st["pending"]], ["b"])
        self.assertIn("a", r2.st["seen"])
        r2.tick()                              # what main() does at startup
        self.assertTrue(self.bot.wait_for(2))
        self.assertEqual([e["id"] for e in self.bot.received[1]["events"]], ["b"])
        self.assertEqual(self.bot.received[1]["pending_count"], 1)


    def test_restart_during_live_run_keeps_old_runs_note(self):
        self.ev({"id": "a"}); self.bot.wait_for(1)
        old = self.bot.received[0]["run_id"]
        end = time.time() + 5
        while self.relay.st["inflight"] and time.time() < end:
            time.sleep(0.02)
        with self.relay.lock:                  # its save to disk has finished too
            pass
        r2 = R.Relay(self.relay.state_path, self.relay.url_file, self.relay.key_file,
                     lease=60, retries=2, backoff=0.01, timeout=5)
        self.assertEqual(r2.event({"id": "b"}), (200, {"status": "forwarded"}))  # 2nd run
        self.assertTrue(self.bot.wait_for(2))
        code, body = r2.done({"run_id": old, "note": "handled a"})
        self.assertEqual(code, 200)
        self.assertEqual(r2.st["last_note"], "handled a")
        self.assertTrue(r2.st["active"])                 # the new run is untouched
        self.assertEqual(r2.done({"run_id": old, "note": "again"})[0], 409)  # only once
        r2.done({"run_id": self.bot.received[1]["run_id"]})
        r2.event({"id": "c"}); self.assertTrue(self.bot.wait_for(3))
        self.assertEqual(self.bot.received[2]["last_note"], "handled a")


class LeaseTest(Base):
    def test_lease_expiry_unwedges_on_event(self):
        self.ev({"id": "a"}); self.ev({"id": "b"}); self.bot.wait_for(1)
        self.expire_lease()                    # run "crashed": never calls /done
        self.assertEqual(self.ev({"id": "c"})[0], 200)
        self.assertTrue(self.bot.wait_for(2))
        got = self.bot.received[1]
        self.assertEqual([e["id"] for e in got["events"]], ["b", "c"])
        self.assertEqual(got["pending_count"], 1)

    def test_lease_expiry_unwedges_on_tick(self):
        self.ev({"id": "a"}); self.bot.wait_for(1)
        self.expire_lease(); self.relay.tick()
        self.assertFalse(self.relay.st["active"])

    def test_tick_flushes_pending_after_expiry(self):
        self.ev({"id": "a"}); self.ev({"id": "b"}); self.bot.wait_for(1)
        self.expire_lease(); self.relay.tick()
        self.assertTrue(self.bot.wait_for(2))
        self.assertEqual(self.bot.received[1]["pending_count"], 1)


class RetryTest(Base):
    bot_kwargs = {"fail_first": 2}

    def test_forward_retries_then_succeeds(self):
        self.ev({"id": "a"})
        self.assertTrue(self.bot.wait_for(1))
        self.assertEqual(self.bot.received[0]["events"], [{"id": "a"}])
        self.assertEqual(self.bot.hits, 3)


class GiveUpTest(Base):
    bot_kwargs = {"fail_first": 4}             # exactly the retry budget

    def test_stranded_events_reforwarded_by_tick(self):
        self.ev({"id": "a"})
        self.assertTrue(self.wait_idle())
        self.assertEqual(self.relay.st["pending"], [{"id": "a"}])
        self.relay.tick()                      # bot is healthy again now
        self.assertTrue(self.bot.wait_for(1))
        self.assertEqual(self.bot.received[0]["events"], [{"id": "a"}])
        self.assertEqual(self.relay.st["pending"], [])


class RedirectTest(unittest.TestCase):
    def test_redirect_is_failure_and_key_not_resent(self):
        target = FakeBot()
        bot = FakeBot(script=[302] * 2, redirect_to=target.url)
        with tempfile.TemporaryDirectory() as d:
            r = R.Relay(os.path.join(d, "s.json"), write(d, "url", bot.url),
                        write(d, "key", KEY), lease=60, retries=2, backoff=0.01, timeout=5)
            self.assertFalse(r._forward({"events": [], "pending_count": 0,
                                         "last_note": None, "run_id": "x"}))
        self.assertEqual(bot.hits, 2)
        self.assertEqual(target.requests, [])  # any method: urllib follows a 302 as GET
        bot.close(); target.close()


class BackoffTest(unittest.TestCase):
    def test_exponential_backoff_with_jitter(self):
        bot = FakeBot(fail_first=10)
        with tempfile.TemporaryDirectory() as d:
            r = R.Relay(os.path.join(d, "s.json"), write(d, "url", bot.url),
                        write(d, "key", KEY), lease=60, retries=4, backoff=1.0, timeout=5)
            with mock.patch.object(R.random, "uniform", side_effect=[0.5, 1.5, 0.7, 1.2]) as u, \
                    mock.patch.object(R.time, "sleep") as s:
                self.assertFalse(r._forward({"events": [], "pending_count": 0,
                                             "last_note": None, "run_id": "x"}))
        bot.close()
        self.assertEqual(bot.hits, 4)
        self.assertEqual([c.args for c in u.call_args_list], [(0.5, 1.5)] * 4)
        delays = [c.args[0] for c in s.call_args_list]
        for got, want in zip(delays, [1 * 0.5, 2 * 1.5, 4 * 0.7, 8 * 1.2], strict=True):
            self.assertAlmostEqual(got, want)


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class CrashTest(unittest.TestCase):
    """A batch the sender got 200 for must survive a kill before the bot has it."""

    def test_kill_after_persist_before_forward_requeues(self):
        bot = FakeBot()
        with tempfile.TemporaryDirectory() as d:
            args = (os.path.join(d, "s.json"), write(d, "url", bot.url), write(d, "key", KEY))
            r = R.Relay(*args, lease=60, retries=2, backoff=0.01, timeout=5)
            with mock.patch.object(R.threading, "Thread"):   # process "dies" pre-forward
                self.assertEqual(r.event({"id": "q"}), (200, {"status": "forwarded"}))
            self.assertEqual(bot.hits, 0)
            r2 = R.Relay(*args, lease=60, retries=2, backoff=0.01, timeout=5)
            self.assertFalse(r2.st["active"])
            self.assertEqual(r2.st["pending"], [{"id": "q"}])
            self.assertEqual(r2.event({"id": "q"}), (200, {"status": "duplicate"}))
            r2.tick()
            self.assertTrue(bot.wait_for(1))
            self.assertEqual(bot.received[0]["events"], [{"id": "q"}])
            time.sleep(0.1)
            self.assertIsNone(r2.st["inflight"])
            r3 = R.Relay(*args, lease=60, retries=2, backoff=0.01, timeout=5)
            self.assertEqual(r3.st["pending"], [])     # delivered: not re-sent again
        bot.close()

    def test_sigkill_mid_retry_then_restart_delivers(self):
        bot = FakeBot(fail_first=1000)                # down while the relay retries
        port = _free_port()
        with tempfile.TemporaryDirectory() as d:
            env = dict(os.environ, RELAY_STATE=os.path.join(d, "s.json"),
                       RELAY_DOWNSTREAM_URL_FILE=write(d, "url", bot.url),
                       RELAY_DOWNSTREAM_KEY_FILE=write(d, "key", KEY),
                       RELAY_PORT=str(port), RELAY_TICK_SECONDS="30", RELAY_BACKOFF="1.0")
            cmd = [sys.executable, os.path.join(os.path.dirname(R.__file__), "relay.py")]

            def start():
                p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True)
                self.assertIn("listening", p.stdout.readline())
                return p
            p = start()
            try:
                self.assertEqual(post(f"http://127.0.0.1:{port}/event", {"id": "q"})[0], 200)
                end = time.time() + 5
                while bot.hits < 1 and time.time() < end:
                    time.sleep(0.02)
                self.assertGreaterEqual(bot.hits, 1)
                p.send_signal(signal.SIGKILL); p.wait()
            finally:
                p.kill(); p.wait()
            bot.script = []                           # bot healthy again
            p = start()                               # startup tick must flush, not wait 30 s
            try:
                self.assertTrue(bot.wait_for(1, timeout=5))
                self.assertEqual(bot.received[0]["events"], [{"id": "q"}])
                self.assertEqual(post(f"http://127.0.0.1:{port}/event", {"id": "q"})[1],
                                 {"status": "duplicate"})
                time.sleep(0.2)
                self.assertEqual(len(bot.received), 1)
            finally:
                p.kill(); p.wait(); p.stdout.close()
        bot.close()


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.args = (os.path.join(d, "s.json"), write(d, "url", "http://127.0.0.1:9/x"),
                     write(d, "key", KEY))
        self.secret = write(d, "secret", SECRET)

    def tearDown(self):
        self.tmp.cleanup()

    def test_retry_budget_must_fit_lease(self):
        with self.assertRaises(ValueError):
            R.Relay(*self.args, lease=30, retries=5, backoff=1.0, timeout=10)
        R.Relay(*self.args, lease=900, retries=5, backoff=1.0, timeout=10)

    def test_non_loopback_bind_needs_secret(self):
        with self.assertRaises(ValueError):
            R.make_server(R.Relay(*self.args), "0.0.0.0", 0)
        srv = R.make_server(R.Relay(*self.args, secret_file=self.secret), "0.0.0.0", 0)
        srv.server_close()
        R.make_server(R.Relay(*self.args), "localhost", 0).server_close()

    def test_ipv6_loopback_bind_serves(self):
        try:                                   # skip only if the host has no ::1 at all
            with socket.socket(socket.AF_INET6) as s:
                s.bind(("::1", 0))
        except OSError:
            self.skipTest("::1 not configured")
        bot = FakeBot()
        write(self.tmp.name, "url", bot.url)
        srv = R.make_server(R.Relay(*self.args), "::1", 0)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            self.assertEqual(post(f"http://[::1]:{srv.server_address[1]}/event", {"id": "6"})[0], 200)
            self.assertTrue(bot.wait_for(1))
        finally:
            srv.shutdown(); srv.server_close(); bot.close()
        with self.assertRaises(ValueError):
            R.make_server(R.Relay(*self.args), "::", 0)


if __name__ == "__main__":
    unittest.main()
