import json, os, signal, subprocess, sys, tempfile, time, unittest
from unittest import mock
from helpers import FakeBot, write
import notify_on_exit as N

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WRAP = os.path.join(ROOT, "notify_on_exit.py")


class NotifyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.bot = FakeBot()
        self.env = dict(os.environ, NOTIFY_URL_FILE=write(self.tmp.name, "url", self.bot.url),
                        NOTIFY_KEY_FILE=write(self.tmp.name, "key", "k"), NOTIFY_BACKOFF="0.01", NOTIFY_RATE_BACKOFF="0.01")
        self.log = os.path.join(self.tmp.name, "job.log")

    def tearDown(self):
        self.bot.close(); self.tmp.cleanup()

    def start(self, shell, fg=True):
        args = [sys.executable, WRAP, "--job", "j1", "--log", self.log]
        args += ["--foreground"] if fg else []
        return subprocess.Popen(args + ["--", "sh", "-c", shell], env=self.env,
                                stdout=subprocess.PIPE, text=True)

    def finish(self, p):
        p.communicate(timeout=10)
        return p.returncode

    def check(self, rc):
        self.assertTrue(self.bot.wait_for(1))
        got = self.bot.received[0]
        self.assertEqual(got["kind"], "job_done")
        self.assertEqual(got["job"], "j1")
        self.assertEqual(got["exit_code"], rc)
        self.assertEqual(got["log_path"], os.path.abspath(self.log))
        self.assertNotIn("SECRET-OUTPUT", json.dumps(got))   # never ships log contents
        self.assertEqual(self.bot.auth[0], "Bearer k")

    def test_rc_zero(self):
        self.assertEqual(self.finish(self.start("echo SECRET-OUTPUT")), 0)
        self.check(0)
        with open(self.log) as f:
            self.assertIn("SECRET-OUTPUT", f.read())

    def test_nonzero_rc(self):
        self.assertEqual(self.finish(self.start("echo SECRET-OUTPUT; exit 3")), 3)
        self.check(3)

    def test_sigterm_reports_true_rc(self):
        p = self.start("sleep 30")
        time.sleep(0.5)
        p.send_signal(signal.SIGTERM)
        self.assertEqual(self.finish(p), 143)
        self.check(143)

    def test_4xx_not_retried(self):
        self.bot.close()
        self.bot = FakeBot(script=[400] * 5)
        write(self.tmp.name, "url", self.bot.url)
        self.assertEqual(self.finish(self.start("exit 0")), 0)
        self.assertEqual((self.bot.hits, self.bot.received), (1, []))

    def test_rate_limit_400_retried(self):
        self.bot.close()
        rl = (400, b'{"error":"Automation run rate limit exceeded"}')
        self.bot = FakeBot(script=[rl, rl, 503, rl])     # more than the normal budget
        write(self.tmp.name, "url", self.bot.url)
        self.assertEqual(self.finish(self.start("exit 0")), 0)
        self.check(0)
        self.assertEqual(self.bot.hits, 5)

    def test_rate_limit_retries_are_bounded(self):
        self.bot.close()
        self.bot = FakeBot(script=[(400, b"rate limit exceeded")] * 20)
        write(self.tmp.name, "url", self.bot.url)
        self.assertEqual(self.finish(self.start("exit 0")), 0)
        self.assertEqual((self.bot.hits, self.bot.received), (6, []))   # 1 + 5 rate retries

    def test_backoff_doubles_with_jitter(self):
        self.bot.close()
        self.bot = FakeBot(fail_first=10)
        with mock.patch.dict(os.environ, self.env), \
                mock.patch.object(N.random, "uniform", side_effect=[0.5, 1.5, 0.7, 1.2]) as u, \
                mock.patch.object(N.time, "sleep") as s:
            write(self.tmp.name, "url", self.bot.url)
            self.assertFalse(N.notify({"id": "x"}, retries=4, backoff=1.0))
        self.assertEqual([c.args for c in u.call_args_list], [(0.5, 1.5)] * 4)
        for got, want in zip([c.args[0] for c in s.call_args_list],
                             [0.5, 3.0, 2.8, 9.6], strict=True):
            self.assertAlmostEqual(got, want)

    def test_5xx_retried(self):
        self.bot.close()
        self.bot = FakeBot(fail_first=2)
        write(self.tmp.name, "url", self.bot.url)
        self.assertEqual(self.finish(self.start("exit 0")), 0)
        self.check(0)
        self.assertEqual(self.bot.hits, 3)
        ids = {r[2]["id"] for r in self.bot.requests}
        self.assertEqual(len(ids), 1)          # same id on every retry, so dedup works
        self.assertTrue(ids.pop().startswith("job-j1-"))

    def test_redirect_not_followed(self):
        target = FakeBot()
        self.bot.close()
        self.bot = FakeBot(script=[302] * 5, redirect_to=target.url)
        write(self.tmp.name, "url", self.bot.url)
        self.assertEqual(self.finish(self.start("exit 0")), 0)
        self.assertEqual(self.bot.hits, 4)
        self.assertEqual(target.requests, [])  # any method: urllib follows a 302 as GET
        target.close()

    def test_relative_log_path_reported_absolute(self):
        p = subprocess.Popen([sys.executable, WRAP, "--job", "j1", "--log", "rel.log",
                              "--foreground", "--", "true"], env=self.env, cwd=self.tmp.name)
        self.assertEqual(self.finish(p), 0)
        self.assertTrue(self.bot.wait_for(1))
        got = self.bot.received[0]["log_path"]
        self.assertTrue(os.path.isabs(got))
        self.assertEqual(os.path.realpath(got), os.path.realpath(os.path.join(self.tmp.name, "rel.log")))

    def test_signal_before_child_starts_is_forwarded(self):
        real, sigs = N.subprocess.Popen, (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)
        saved = {s: signal.getsignal(s) for s in sigs}

        def popen(*a, **k):
            os.kill(os.getpid(), signal.SIGTERM)   # handled while child is still None
            return real(*a, **k)
        try:
            with mock.patch.object(N.subprocess, "Popen", popen), \
                    mock.patch.dict(os.environ, self.env):
                t = time.time()
                rc = N.run("j1", self.log, ["sleep", "5"])
        finally:
            for s, h in saved.items():
                signal.signal(s, h)
        self.assertEqual(rc, 143)
        self.assertLess(time.time() - t, 4)
        self.check(143)

    def test_detached_mode_returns_immediately(self):
        t = time.time()
        p = self.start("sleep 0.5; exit 7", fg=False)
        out, _ = p.communicate(timeout=5)
        self.assertEqual(p.returncode, 0)
        self.assertLess(time.time() - t, 0.5 + 2)
        self.assertTrue(out.strip().isdigit())
        self.check(7)


if __name__ == "__main__":
    unittest.main()
