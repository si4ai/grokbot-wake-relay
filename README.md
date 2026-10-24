# grokbot-shim

A small stdlib-only Python reference for webhook-woken bot routines. It shows the
wake behavior we'd like the platform to provide natively, and it works as a
stopgap until then.

## The problem

1. Every webhook POST starts its own cold run. A burst of 5 events gives 5 parallel runs
   racing over the same work, replying twice and redoing each other's work.
2. A run doesn't know what the previous run did.
3. A background job started in a run finishes silently, and nothing wakes the bot to review it.

## The fix, as two pieces

**`relay.py`** sits between upstream senders and the bot's webhook, and holds a little
state per routine (`active`, `pending`, `last_note`) in a JSON file.
- Event while idle: forward it now as `{events, pending_count: 0, last_note, run_id}` and mark the routine active.
- Event while active: queue it and return `202`.
- `POST /done {note}` from the run: save the note. If anything is pending, send it all as
  **one** batched wake. Otherwise go idle.
- Lease timeout (default 15 min): if a run crashes without calling `/done`, the next event or tick un-wedges it.
- Duplicate event `id`s are dropped, and forwards retry with exponential backoff and jitter.
  After the last retry fails, the events go back to pending and the routine goes idle.

**`notify_on_exit.py`** runs a command detached. When it exits, it POSTs
`{kind: "job_done", job, exit_code, log_path}` to the relay's `/event` or to the bot webhook.
It sends only the exit code and log path, never log contents. A signal-killed job reports
`128+N`, so SIGTERM gives `143`.

```sh
notify_on_exit.py --job nightly-sweep --log /tmp/sweep.log -- ./sweep.sh
```

## The routine's one extra step

As the last step of every run, call `POST /done` with
`{"note": "<what I did / what's next>", "run_id": "<from the wake>"}`. The next wake
includes that note as `last_note`. A `/done` carrying a stale `run_id` gets `409`.

## Config

The relay reads its config from env vars. Secrets are always read from files and never printed:
`RELAY_DOWNSTREAM_URL_FILE`, `RELAY_DOWNSTREAM_KEY_FILE`, optional `RELAY_INBOUND_SECRET_FILE`
(callers send `Authorization: Bearer <secret>`), `RELAY_STATE`, `RELAY_LEASE_SECONDS`,
`RELAY_TICK_SECONDS`, `RELAY_BIND` (default `127.0.0.1`) and `RELAY_PORT`.
The wrapper uses `NOTIFY_URL_FILE` and an optional `NOTIFY_KEY_FILE`.

## Limits (why this should be native)

- The relay **can't push events into a run that's already going**. It can only batch them
  for the next run. A native fix could deliver them to the live run, or let the runner
  hand over a pending count.
- `last_note` is only as good as what the run writes. A native per-routine state slot
  would be loaded automatically.
- A run that crashes delays queued events until the lease expires.
- One relay process serves one routine. A SIGKILLed wrapper can't report, and nothing can catch SIGKILL.
- It isn't hardened for the public internet. Run it behind TLS and your own auth.

## Run

```sh
python -m unittest discover -s tests -v   # 16 tests, about 12 s
python demo.py                            # localhost-only end-to-end demo
```

MIT licensed.
