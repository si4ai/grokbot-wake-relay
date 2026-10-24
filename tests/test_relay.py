import os, tempfile, threading, time, unittest
from helpers import FakeBot, write, post
import relay as R

KEY = "test-downstream-key"
SECRET = "test-inbound-secret"


class Base(unittest.TestCase):
    def setUp(self, lease=60.0, fail_first=0):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.bot = FakeBot(fail_first=fail_first)
        self.relay = R.Relay(os.path.join(d, "state.json"), write(d, "url", self.bot.url),
                             write(d, "key", KEY), write(d, "secret", SECRET),
                             lease=lease, retries=4, backoff=0.01)
        self.srv = R.make_server(self.relay, "127.0.0.1", 0)
        self.base = f"http://127.0.0.1:{self.srv.server_address[1]}"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def tearDown(self):
        self.srv.shutdown(); self.srv.server_close(); self.bot.close(); self.tmp.cleanup()

    def ev(self, body):
        return post(self.base + "/event", body, SECRET)

    def done(self, body=None):
        return post(self.base + "/done", body or {}, SECRET)


class RelayTest(Base):

    def test_idle_event_forwards_immediately(self):
        code, _ = self.ev({"id": "a", "x": 1})
        self.assertEqual(code, 200)
        self.assertTrue(self.bot.wait_for(1))
        got = self.bot.received[0]
        self.assertEqual(got["events"], [{"id": "a", "x": 1}])
        self.assertEqual(got["pending_count"], 0)
        self.assertIsNone(got["last_note"])
        self.assertEqual(self.bot.auth[0], "Bearer " + KEY)
        self.assertTrue(self.relay.st["active"])

    def test_burst_of_five_coalesces(self):
        codes = [self.ev({"id": f"e{i}"})[0] for i in range(5)]
        self.assertEqual(codes, [200, 202, 202, 202, 202])
        self.assertTrue(self.bot.wait_for(1))
        time.sleep(0.2)
        self.assertEqual(len(self.bot.received), 1)
        code, body = self.done({"note": "handled e0"})
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
        self.assertEqual(self.done({"note": "n1"}), (200, {"status": "idle"}))
        self.assertFalse(self.relay.st["active"])
        self.ev({"id": "b"}); self.assertTrue(self.bot.wait_for(2))
        self.assertEqual(self.bot.received[1]["last_note"], "n1")

    def test_stale_run_id_rejected(self):
        self.ev({"id": "a"}); self.bot.wait_for(1)
        self.assertEqual(self.done({"run_id": "nope"})[0], 409)
        rid = self.bot.received[0]["run_id"]
        self.assertEqual(self.done({"run_id": rid})[1]["status"], "idle")

    def test_duplicate_event_id_deduped(self):
        self.ev({"id": "same"}); self.bot.wait_for(1)
        self.assertEqual(self.ev({"id": "same"})[1]["status"], "duplicate")
        self.assertEqual(self.relay.st["pending"], [])
        self.done(); time.sleep(0.1)
        self.assertEqual(len(self.bot.received), 1)

    def test_unauthorized_rejected(self):
        self.assertEqual(post(self.base + "/event", {"id": "x"}, "wrong")[0], 401)
        self.assertEqual(post(self.base + "/event", {"id": "x"})[0], 401)

    def test_state_survives_restart(self):
        self.ev({"id": "a"}); self.ev({"id": "b"}); self.bot.wait_for(1)
        r2 = R.Relay(self.relay.state_path, self.relay.url_file, self.relay.key_file)
        self.assertTrue(r2.st["active"])
        self.assertEqual([e["id"] for e in r2.st["pending"]], ["b"])
        self.assertIn("a", r2.st["seen"])


class LeaseTest(Base):
    def setUp(self):
        super().setUp(lease=0.3)

    def test_lease_expiry_unwedges_on_event(self):
        self.ev({"id": "a"}); self.ev({"id": "b"}); self.bot.wait_for(1)
        time.sleep(0.4)                       # run "crashed": never calls /done
        self.assertEqual(self.ev({"id": "c"})[0], 200)
        self.assertTrue(self.bot.wait_for(2))
        got = self.bot.received[1]
        self.assertEqual([e["id"] for e in got["events"]], ["b", "c"])
        self.assertEqual(got["pending_count"], 1)

    def test_lease_expiry_unwedges_on_tick(self):
        self.ev({"id": "a"}); self.bot.wait_for(1)
        time.sleep(0.4); self.relay.tick()
        self.assertFalse(self.relay.st["active"])

    def test_tick_flushes_pending_after_expiry(self):
        self.ev({"id": "a"}); self.ev({"id": "b"}); self.bot.wait_for(1)
        time.sleep(0.4); self.relay.tick()
        self.assertTrue(self.bot.wait_for(2))
        self.assertEqual(self.bot.received[1]["pending_count"], 1)


class RetryTest(Base):
    def setUp(self):
        super().setUp(fail_first=2)

    def test_forward_retries_then_succeeds(self):
        self.ev({"id": "a"})
        self.assertTrue(self.bot.wait_for(1))
        self.assertEqual(self.bot.received[0]["events"], [{"id": "a"}])


class GiveUpTest(Base):
    def setUp(self):
        super().setUp(fail_first=100)

    def test_failed_forward_requeues_and_goes_idle(self):
        self.ev({"id": "a"})
        end = time.time() + 3
        while time.time() < end and self.relay.st["active"]:
            time.sleep(0.02)
        self.assertFalse(self.relay.st["active"])
        self.assertEqual(self.relay.st["pending"], [{"id": "a"}])


if __name__ == "__main__":
    unittest.main()
