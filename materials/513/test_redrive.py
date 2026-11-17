"""End-to-end verification for task 513.

Runs against real SQLite rows in a temporary directory.  The supplied
materials' normal demo path is covered too: initial +5, duplicate delivery, an
invalid value type and a manual fact edit.

Additional scenarios injected here:
  * crash after the fact/effect commit but before ACK, then recover
  * a late settlement from an old delivery while a new one is inflight
  * verbatim (original) redrive of a dead message
  * a corrected redrive whose stale expected_row_version must not overwrite a
    human edit, followed by an explicit new expected version
"""

import json
import os
import sqlite3
import tempfile
from pathlib import Path

from deadletter_service import DeadLetterService
from event_loader import open_store

PASS = []
FAIL = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS " if cond else "FAIL ") + name + (" :: " + str(detail) if detail else ""))


def fresh():
    root = tempfile.mkdtemp(prefix="dlq513-")
    return Path(root) / "store.db"


def scenario_basic(db):
    """+5, duplicate delivery, invalid type, manual edit -- real rows."""
    s = DeadLetterService(db)
    e1 = {"origin_id": "e1", "revision": 1, "key": "a", "delta": 5,
          "expected_row_version": 0}
    r1 = s.submit(e1)
    check("initial +5 applied", r1["final_state"] == "acked"
          and s.fact("a") == {"key": "a", "value": 5, "version": 1}, s.fact("a"))

    dup = s.submit(dict(e1))
    check("duplicate delivery replays first result",
          dup["result"]["status"] == "replayed"
          and dup["result"]["value"] == 5
          and dup["result"]["first_delivery_id"] == r1["delivery_id"],
          dup["result"])
    check("duplicate did not add again",
          s.fact("a") == {"key": "a", "value": 5, "version": 1}, s.fact("a"))
    check("duplicate got a new delivery id",
          dup["delivery_id"] != r1["delivery_id"])
    check("exactly one effect receipt", s.effect_count("e1") == 1)

    bad = {"origin_id": "e2", "revision": 1, "key": "a", "delta": 6,
           "expected_row_version": 1}
    bad["delta"] = "bad"
    rb = s.submit(bad)
    check("value-type error is permanent dead",
          rb["final_state"] == "dead"
          and rb["failure_class"] == "permanent"
          and s.state("e2", 1)["failure_class"] == "permanent", rb["failure_class"])
    check("permanent error had no effect",
          s.fact("a")["value"] == 5 and s.effect_count("e2") == 0)

    # Manual edit of the facts row.
    with db:
        db.execute("update facts set value=value+100, version=version+1 where key='a'")
    check("manual edit visible", s.fact("a") == {"key": "a", "value": 105, "version": 2},
          s.fact("a"))

    # Duplicate delivery after the row changed must still replay, not re-add
    # and not be re-evaluated as stale.
    dup2 = s.submit(dict(e1))
    check("late duplicate still replays despite changed row",
          dup2["result"]["status"] == "replayed" and dup2["result"]["value"] == 5,
          dup2["result"])
    check("late duplicate did not touch manual value",
          s.fact("a") == {"key": "a", "value": 105, "version": 2}, s.fact("a"))

    # Receipts and versions in raw SQLite rows.
    eff = db.execute("select origin_id, revision, value_before, version_before,"
                     " value_after, version_after from effects order by origin_id").fetchall()
    check("raw effect row records original identity and version",
          eff == [("e1", 1, 0, 0, 5, 1)], eff)
    dm = db.execute("select attempt_no, status from deliveries where origin_id='e1'"
                    " order by attempt_no").fetchall()
    check("all deliveries retained for e1",
          dm == [(1, "acked"), (2, "acked"), (3, "acked")], dm)
    return s


def scenario_crash_before_ack(path):
    """Commit effect, then crash before the delivery is settled."""
    db = open_store(path)
    s = DeadLetterService(db)
    ev = {"origin_id": "c1", "revision": 1, "key": "crash", "delta": 7,
          "expected_row_version": 0}

    def explode(delivery_id, outcome):
        raise RuntimeError("process exit after commit, before ACK")

    try:
        s.submit(ev, on_after_apply=explode)
    except RuntimeError as exc:
        check("simulated crash after commit before ACK", "before ACK" in str(exc))
    st = s.state("c1", 1)
    check("effect committed, delivery still inflight",
          s.fact("crash") == {"key": "crash", "value": 7, "version": 1}
          and s.effect_count("c1") == 1
          and st["state"] == "inflight", (s.fact("crash"), st["state"]))

    actions = s.recover()
    check("recovery reads receipt and acks",
          any(a["action"] == "ack_from_receipt" for a in actions)
          and s.state("c1", 1)["state"] == "acked", actions)
    check("recovery did not re-apply the delta",
          s.fact("crash")["value"] == 7 and s.effect_count("c1") == 1,
          s.fact("crash"))

    # A concurrent duplicate during recovery would also replay.
    again = s.submit(dict(ev))
    check("post-recovery duplicate replays", again["result"]["status"] == "replayed"
          and s.fact("crash")["value"] == 7)


def scenario_late_delivery(path):
    """Old delivery's late ACK must not touch the new inflight attempt."""
    db = open_store(path)
    s = DeadLetterService(db)
    ev = {"origin_id": "l1", "revision": 1, "key": "late", "delta": 4,
          "expected_row_version": 0}

    def explode(delivery_id, outcome):
        raise RuntimeError("drop ACK")

    try:
        s.submit(ev, on_after_apply=explode)
    except RuntimeError:
        pass
    old = s.state("l1", 1)["current_delivery_id"]
    resent = s.timeout_resend(old)
    new = s.state("l1", 1)["current_delivery_id"]
    check("timeout resend creates a new delivery id", new and new != old,
          (old, new))

    late = s.settle(old, {"status": "replayed", "value": 4, "version": 1})
    st = s.state("l1", 1)
    check("late settlement of old delivery is ignored",
          late["status"] == "ignored" and st["current_delivery_id"] == new
          and st["state"] == "inflight", (late, st["state"]))
    check("new inflight delivery untouched",
          s.delivery(new)["status"] == "inflight")
    check("old delivery marked superseded/retried",
          s.delivery(old)["status"] == "retried"
          and s.delivery(old)["failure_class"] == "superseded")

    # Recover the current attempt from the durable receipt.
    s.recover()
    check("recovery acks the new delivery without re-adding",
          s.state("l1", 1)["state"] == "acked"
          and s.fact("late")["value"] == 4
          and s.effect_count("l1") == 1, s.fact("late"))


def scenario_original_redrive(db):
    """Dead (stale) message redriven verbatim: same identity and body."""
    s = DeadLetterService(db)
    ev = {"origin_id": "r1", "revision": 1, "key": "r", "delta": 5,
          "expected_row_version": 9}
    first = s.submit(ev)
    check("stale message is dead", first["final_state"] == "dead"
          and first["failure_class"] == "stale", first["failure_class"])
    before = s.message("r1", 1)
    again = s.redrive("r1", 1, "original")
    after = s.message("r1", 1)
    check("original redrive keeps identity and body",
          before == after and again["revision"] == 1, (before, after))
    check("original redrive still evaluates row version",
          again["final_state"] == "dead" and s.fact("r") is None,
          again["final_state"])

    # Human corrects the row to version 9; the identical original now applies.
    with db:
        db.execute("insert into facts values('r', 90, 9)")
    ok = s.redrive("r1", 1, "original")
    check("original redrive applies once row version matches",
          ok["final_state"] == "acked" and s.fact("r") == {"key": "r", "value": 95, "version": 10},
          s.fact("r"))
    hist = s.history("r1")
    check("delivery history shows 3 attempts, one revision",
          len(hist["deliveries"]) == 3 and len(hist["messages"]) == 1, hist)


def scenario_corrected_redrive(db):
    """Corrected redrive creates the next revision with predecessor link."""
    s = DeadLetterService(db)
    bad = {"origin_id": "x1", "revision": 1, "key": "x", "delta": True,
           "expected_row_version": 0}
    rb = s.submit(bad)
    check("bool delta is permanent", rb["final_state"] == "dead"
          and rb["failure_class"] == "permanent", rb["result"])

    # No expected_row_version: administrator must give one explicitly.
    try:
        s.redrive("x1", 1, "corrected", {"key": "x", "delta": 6})
    except Exception as exc:
        check("correction without explicit version rejected",
              "expected_row_version" in str(exc), str(exc))

    # Human changes the row since the failure.
    with db:
        db.execute("insert into facts values('x', 50, 4)")

    # Old expected version cannot overwrite the human value.
    st1 = s.redrive("x1", 1, "corrected",
                    {"key": "x", "delta": 6, "expected_row_version": 0})
    check("correction with stale version does not overwrite",
          st1["final_state"] == "dead" and st1["failure_class"] == "stale"
          and s.fact("x") == {"key": "x", "value": 50, "version": 4},
          (st1["final_state"], s.fact("x")))
    check("stale correction created revision 2 with predecessor",
          s.message("x1", 2)["predecessor"] == ("x1", 1)
          and s.message("x1", 2)["is_correction"])

    # Explicit new expected version continues.
    ok = s.redrive("x1", 2, "corrected",
                   {"key": "x", "delta": 6, "expected_row_version": 4})
    check("correction with explicit new version applies",
          ok["final_state"] == "acked"
          and s.fact("x") == {"key": "x", "value": 56, "version": 5},
          s.fact("x"))

    # Cannot correct over an existing success receipt.
    try:
        s.redrive("x1", 1, "corrected",
                  {"key": "x", "delta": 1, "expected_row_version": 5})
    except Exception as exc:
        check("correction over success receipt refused",
              "success receipt" in str(exc), str(exc))

    # Cannot rewrite the originally stored message body.
    raw = db.execute("select body_json from messages where origin_id='x1'"
                     " and revision=1").fetchone()[0]
    check("original message row still has the bad body",
          json.loads(raw)["delta"] is True, raw)
    hist = s.history("x1")
    check("history exposes revisions and deliveries",
          [m["revision"] for m in hist["messages"]] == [1, 2, 3]
          and len(hist["deliveries"]) >= 3, hist["messages"])

    check("only revision 3 has an effect",
          [e["revision"] for e in hist["effects"]] == [3], hist["effects"])


def scenario_retryable(db):
    """Controlled busy is retryable, no effect, and returns to pending."""
    from event_loader import RetryableBusyError
    s = DeadLetterService(db, max_attempts=2)
    base = s.store.apply
    state = {"n": 0}

    def flaky(body, delivery_id=None):
        state["n"] += 1
        if state["n"] == 1:
            raise RetryableBusyError("database is locked")
        return base(body, delivery_id)

    s.store.apply = flaky
    ev = {"origin_id": "b1", "revision": 1, "key": "b", "delta": 3,
          "expected_row_version": 0}
    first = s.submit(ev)
    check("busy is retryable and returns to pending",
          first["final_state"] == "pending" and s.fact("b") is None
          and s.state("b1", 1)["state"] == "pending", first["final_state"])
    retried = s.process_pending()
    check("pending retry applies once",
          retried and retried[0]["final_state"] == "acked"
          and s.fact("b") == {"key": "b", "value": 3, "version": 1},
          s.fact("b"))
    check("effect count is one", s.effect_count("b1") == 1)


def scenario_independent_keys(db):
    """A blocked message on one key must not block another key."""
    from event_loader import RetryableBusyError
    s = DeadLetterService(db)
    base = s.store.apply

    def only_busy_on_z(body, delivery_id=None):
        if body.get("key") == "blocked":
            raise RetryableBusyError("locked")
        return base(body, delivery_id)

    s.store.apply = only_busy_on_z
    b = s.submit({"origin_id": "p1", "revision": 1, "key": "blocked",
                  "delta": 1, "expected_row_version": 0})
    check("blocked key stays pending", b["final_state"] == "pending", b["final_state"])
    ok = s.submit({"origin_id": "p2", "revision": 1, "key": "free",
                   "delta": 2, "expected_row_version": 0})
    check("other key still delivers", ok["final_state"] == "acked"
          and s.fact("free") == {"key": "free", "value": 2, "version": 1},
          s.fact("free"))


def main():
    path = fresh()
    db = open_store(path)
    scenario_basic(db)
    db.close()

    scenario_crash_before_ack(fresh())
    scenario_late_delivery(fresh())

    db = open_store(fresh())
    scenario_original_redrive(db)
    db.close()

    db = open_store(fresh())
    scenario_corrected_redrive(db)
    db.close()

    db = open_store(fresh())
    scenario_retryable(db)
    scenario_independent_keys(db)
    db.close()

    print("\n%d passed, %d failed" % (len(PASS), len(FAIL)))
    if FAIL:
        print("failed:", FAIL)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
