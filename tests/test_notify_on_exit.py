import json, os, signal, subprocess, sys, tempfile, time, unittest
from helpers import FakeBot, write

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WRAP = os.path.join(ROOT, "notify_on_exit.py")


class NotifyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.bot = FakeBot()
        self.env = dict(os.environ, NOTIFY_URL_FILE=write(self.tmp.name, "url", self.bot.url),
                        NOTIFY_KEY_FILE=write(self.tmp.name, "key", "k"), NOTIFY_BACKOFF="0.01")
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
