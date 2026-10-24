import http.client, json, os, tempfile, threading, time, unittest
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
        r2 = R.Relay(self.relay.state_path, self.relay.url_file, self.relay.key_file,
                     lease=60, retries=2, backoff=0.01, timeout=5)
        self.assertFalse(r2.st["active"])
        self.assertEqual([e["id"] for e in r2.st["pending"]], ["b"])
        self.assertIn("a", r2.st["seen"])
        r2.tick()                              # what main() does at startup
        self.assertTrue(self.bot.wait_for(2))
        self.assertEqual([e["id"] for e in self.bot.received[1]["events"]], ["b"])
        self.assertEqual(self.bot.received[1]["pending_count"], 1)


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
        self.assertEqual((bot.hits, target.hits), (2, 0))
        bot.close(); target.close()


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


if __name__ == "__main__":
    unittest.main()
