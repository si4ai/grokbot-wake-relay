#!/usr/bin/env python3
"""End-to-end demo on localhost only: relay + fake bot webhook, 5 events in 2 s, /done."""
import json, os, sys, tempfile, threading, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests"))
from helpers import FakeBot, write, post  # noqa: E402
import relay as R  # noqa: E402


def main():
    with tempfile.TemporaryDirectory() as d:
        bot = FakeBot()
        rl = R.Relay(os.path.join(d, "state.json"), write(d, "url", bot.url),
                     write(d, "key", "demo-key"), lease=60, backoff=0.05)
        srv = R.make_server(rl, "127.0.0.1", 0)
        base = f"http://127.0.0.1:{srv.server_address[1]}"
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        print(f"relay on {base}, fake bot on 127.0.0.1:{bot.srv.server_address[1]}")
        t0 = time.time()
        for i in range(5):
            code, body = post(base + "/event", {"id": f"evt-{i}", "text": f"event {i}"})
            print(f"t={time.time() - t0:4.1f}s POST /event evt-{i} -> {code} {body}")
            time.sleep(0.4)
        bot.wait_for(1)
        print("run 1 finishes and calls /done ->", post(base + "/done", {"note": "handled evt-0"}))
        bot.wait_for(2)
        print("run 2 finishes and calls /done ->", post(base + "/done", {"note": "handled evt-1..4"}))
        print(f"\nfake bot saw {len(bot.received)} forwards (5 events):")
        for i, f in enumerate(bot.received, 1):
            ids = [e["id"] for e in f["events"]]
            print(f"  wake {i}: events={ids} pending_count={f['pending_count']} "
                  f"last_note={json.dumps(f['last_note'])}")
        srv.shutdown(); bot.close()


if __name__ == "__main__":
    main()
