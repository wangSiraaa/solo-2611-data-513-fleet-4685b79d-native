"""Durable delivery / dead-letter service with identity-preserving redrive.

Model
-----
* A *business message* is identified by ``(origin_id, revision)`` and its body
  (key, delta, expected_row_version) is immutable in the ``messages`` inbox.
* A *delivery* is one attempt: it has its own ``delivery_id``.  A timeout
  resend, a duplicate delivery or a redrive always creates a new delivery while
  keeping the same message identity.
* ``message_state`` holds pending / inflight / acked / dead plus the failure
  class.  ``deliveries`` keeps every historical attempt.  ``effects`` (owned by
  the loader) is the durable success receipt and is the only authority for
  deciding an otherwise unknown outcome.

Everything lives in one SQLite database by default.  The effect store is
pluggable; if the effect receipt lives elsewhere, the ledger deliberately does
*not* claim exactly-once across databases.  Recovery must read the effect
receipt first: an ``inflight`` or missing delivery settlement is ambiguous and
is never treated as an explicit business rejection.
"""

import json
import sqlite3
import uuid
from datetime import datetime, timezone

if __package__:
    from .event_loader import (
        INT64_MAX,
        INT64_MIN,
        EventLoader,
        RetryableBusyError,
        canonical_body,
        open_store,
        validation_error,
    )
else:
    from event_loader import (
        INT64_MAX,
        INT64_MIN,
        EventLoader,
        RetryableBusyError,
        canonical_body,
        open_store,
        validation_error,
    )

MAX_DELIVERIES = 5       # hard scope cap: at most 5 attempts per original
MAX_REVISIONS = 3        # at most 3 revisions of one origin
DEFAULT_MAX_ATTEMPTS = 3 # auto-retry budget before a retryable failure is dead

DL_SCHEMA = """
create table if not exists messages(
    origin_id text not null,
    revision integer not null,
    key text,
    delta_json text,
    expected_row_version integer,
    body_json text not null,
    predecessor_origin_id text,
    predecessor_revision integer,
    is_correction integer not null default 0,
    created_at text not null,
    primary key(origin_id, revision)
);
create table if not exists message_state(
    origin_id text not null,
    revision integer not null,
    state text not null check(state in ('pending','inflight','acked','dead')),
    current_delivery_id text,
    attempt_count integer not null default 0,
    episode_attempts integer not null default 0,
    failure_class text,
    reason text,
    result_json text,
    updated_at text not null,
    primary key(origin_id, revision)
);
create table if not exists deliveries(
    delivery_id text primary key,
    origin_id text not null,
    revision integer not null,
    attempt_no integer not null,
    trigger text not null,
    status text not null check(status in ('pending','inflight','acked','dead','retried')),
    failure_class text,
    reason text,
    result_json text,
    created_at text not null,
    settled_at text,
    foreign key(origin_id, revision) references messages(origin_id, revision)
);
create index if not exists deliveries_status_idx on deliveries(status);
create index if not exists deliveries_identity_idx on deliveries(origin_id, revision);
create table if not exists effects(
    origin_id text not null,
    revision integer not null,
    key text not null,
    value_before integer not null,
    version_before integer not null,
    value_after integer not null,
    version_after integer not null,
    first_delivery_id text,
    result_json text not null,
    created_at text not null,
    primary key(origin_id, revision)
);
create trigger if not exists messages_no_update
before update on messages
begin
    select raise(abort, 'message records are immutable');
end;
create trigger if not exists messages_no_delete
before delete on messages
begin
    select raise(abort, 'message records are immutable');
end;
create trigger if not exists effects_no_update
before update on effects
begin
    select raise(abort, 'effect receipts are immutable');
end;
create trigger if not exists effects_no_delete
before delete on effects
begin
    select raise(abort, 'effect receipts are immutable');
end;
"""


class IdentityBodyConflict(Exception):
    """Same (origin_id, revision) was presented with a different body."""


class InvalidRedrive(Exception):
    """The requested redrive is not legal (wrong state, revision, etc.)."""


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _is_busy(exc):
    text = str(exc).lower()
    return "locked" in text or "busy" in text


class DeadLetterService:
    def __init__(self, db, store=None, max_attempts=DEFAULT_MAX_ATTEMPTS):
        self.db = db
        self.db.executescript(DL_SCHEMA)
        self.store = store if store is not None else EventLoader(db)
        self.max_attempts = max_attempts
        if isinstance(self.store, EventLoader):
            self.store.db = self.db

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------
    def state(self, origin_id, revision):
        row = self.db.execute(
            "select origin_id, revision, state, current_delivery_id, attempt_count,"
            " episode_attempts, failure_class, reason, result_json from message_state"
            " where origin_id=? and revision=?",
            (origin_id, revision),
        ).fetchone()
        if row is None:
            return None
        return {
            "origin_id": row[0],
            "revision": row[1],
            "state": row[2],
            "current_delivery_id": row[3],
            "attempt_count": row[4],
            "episode_attempts": row[5],
            "failure_class": row[6],
            "reason": row[7],
            "result": json.loads(row[8]) if row[8] else None,
        }

    def delivery(self, delivery_id):
        row = self.db.execute(
            "select delivery_id, origin_id, revision, attempt_no, trigger, status,"
            " failure_class, reason, result_json, created_at, settled_at"
            " from deliveries where delivery_id=?",
            (delivery_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "delivery_id": row[0],
            "origin_id": row[1],
            "revision": row[2],
            "attempt_no": row[3],
            "trigger": row[4],
            "status": row[5],
            "failure_class": row[6],
            "reason": row[7],
            "result": json.loads(row[8]) if row[8] else None,
            "created_at": row[9],
            "settled_at": row[10],
        }

    def message(self, origin_id, revision):
        row = self.db.execute(
            "select origin_id, revision, body_json, predecessor_origin_id,"
            " predecessor_revision, is_correction from messages"
            " where origin_id=? and revision=?",
            (origin_id, revision),
        ).fetchone()
        if row is None:
            return None
        return {
            "origin_id": row[0],
            "revision": row[1],
            "body": json.loads(row[2]),
            "predecessor": (row[3], row[4]),
            "is_correction": bool(row[5]),
        }

    def history(self, origin_id):
        messages = self.db.execute(
            "select origin_id, revision, body_json, predecessor_revision,"
            " is_correction from messages where origin_id=? order by revision",
            (origin_id,),
        ).fetchall()
        deliveries = self.db.execute(
            "select delivery_id, revision, attempt_no, trigger, status,"
            " failure_class, reason from deliveries where origin_id=?"
            " order by created_at, rowid",
            (origin_id,),
        ).fetchall()
        effects = self.db.execute(
            "select revision, value_before, version_before, value_after,"
            " version_after, first_delivery_id from effects where origin_id=?"
            " order by revision",
            (origin_id,),
        ).fetchall()
        return {
            "messages": [
                {
                    "revision": r[1],
                    "body": json.loads(r[2]),
                    "predecessor_revision": r[3],
                    "is_correction": bool(r[4]),
                }
                for r in messages
            ],
            "deliveries": [
                {
                    "delivery_id": r[0],
                    "revision": r[1],
                    "attempt_no": r[2],
                    "trigger": r[3],
                    "status": r[4],
                    "failure_class": r[5],
                    "reason": r[6],
                }
                for r in deliveries
            ],
            "effects": [
                {
                    "revision": r[0],
                    "value_before": r[1],
                    "version_before": r[2],
                    "value_after": r[3],
                    "version_after": r[4],
                    "first_delivery_id": r[5],
                }
                for r in effects
            ],
        }

    def stats(self):
        states = dict(
            self.db.execute(
                "select state, count(*) from message_state group by state"
            ).fetchall()
        )
        dead = dict(
            self.db.execute(
                "select coalesce(failure_class,'unknown'), count(*) from message_state"
                " where state='dead' group by failure_class"
            ).fetchall()
        )
        return {
            "pending": states.get("pending", 0),
            "inflight": states.get("inflight", 0),
            "acked": states.get("acked", 0),
            "dead": states.get("dead", 0),
            "dead_by_class": dead,
        }

    def fact(self, key):
        row = self.db.execute(
            "select key, value, version from facts where key=?", (key,)
        ).fetchone()
        return None if row is None else {"key": row[0], "value": row[1], "version": row[2]}

    def effect_count(self, origin_id=None):
        if origin_id is None:
            return self.db.execute("select count(*) from effects").fetchone()[0]
        return self.db.execute(
            "select count(*) from effects where origin_id=?", (origin_id,)
        ).fetchone()[0]

    # ------------------------------------------------------------------
    # ingestion
    # ------------------------------------------------------------------
    def submit(self, event, delivery_id=None, trigger="ingest",
               on_after_apply=None):
        """Ingest a business message and make one delivery attempt.

        A duplicate submission of an existing identity must carry identical
        business fields; it gets a fresh delivery but the loader replays the
        first receipt instead of adding the value again.
        """
        if not isinstance(event, dict):
            return {"status": "permanent", "reason": "invalid_body_type"}
        canonical = canonical_body(event)
        origin_id = event.get("origin_id")
        revision = event.get("revision")
        if not isinstance(origin_id, str) or not origin_id:
            return {"status": "permanent", "reason": "invalid_origin_id"}
        if type(revision) is not int or revision < 1:
            return {"status": "permanent", "reason": "invalid_revision"}

        try:
            self.db.execute("begin immediate")
            row = self.db.execute(
                "select body_json from messages where origin_id=? and revision=?",
                (origin_id, revision),
            ).fetchone()
            if row is not None:
                if row[0] != canonical:
                    self.db.execute("rollback")
                    return {
                        "status": "permanent",
                        "reason": "identity_body_conflict",
                        "origin_id": origin_id,
                        "revision": revision,
                    }
                current = self.state(origin_id, revision)
                if current["state"] == "dead" and trigger != "redrive":
                    self.db.execute("rollback")
                    return {
                        "status": "dead",
                        "reason": "message_dead_use_redrive",
                        "origin_id": origin_id,
                        "revision": revision,
                    }
            else:
                if revision > MAX_REVISIONS:
                    self.db.execute("rollback")
                    return {"status": "permanent", "reason": "revision_limit"}
                self.db.execute(
                    "insert into messages(origin_id, revision, key, delta_json,"
                    " expected_row_version, body_json, created_at)"
                    " values(?,?,?,?,?,?,?)",
                    (
                        origin_id,
                        revision,
                        event.get("key"),
                        json.dumps(event.get("delta"), default=repr),
                        event.get("expected_row_version"),
                        canonical,
                        _now(),
                    ),
                )
                self.db.execute(
                    "insert into message_state(origin_id, revision, state,"
                    " attempt_count, updated_at) values(?,?,'pending',0,?)",
                    (origin_id, revision, _now()),
                )
            self.db.execute("commit")
        except sqlite3.OperationalError as exc:
            if self.db.in_transaction:
                self.db.execute("rollback")
            if _is_busy(exc):
                return {"status": "retryable", "reason": "storage_busy"}
            raise

        did, err = self._create_delivery(origin_id, revision, trigger, delivery_id)
        if did is None:
            return {"status": "rejected", "reason": err, "origin_id": origin_id,
                    "revision": revision}
        return self.deliver(did, on_after_apply=on_after_apply)

    # ------------------------------------------------------------------
    # delivery
    # ------------------------------------------------------------------
    def _create_delivery(self, origin_id, revision, trigger, delivery_id=None):
        did = delivery_id or "d-" + uuid.uuid4().hex
        try:
            self.db.execute("begin immediate")
            if self.delivery(did) is not None:
                self.db.execute("rollback")
                return None, "duplicate_delivery_id"
            st = self.state(origin_id, revision)
            if st is None:
                self.db.execute("rollback")
                return None, "unknown_message"
            if st["attempt_count"] >= MAX_DELIVERIES:
                self.db.execute("rollback")
                return None, "delivery_limit"
            if (self.max_attempts is not None
                    and trigger == "retry"
                    and st["state"] == "pending"
                    and st["episode_attempts"] >= self.max_attempts):
                self.db.execute("rollback")
                return None, "retry_exhausted"
            if st["state"] == "dead" and trigger != "redrive":
                self.db.execute("rollback")
                return None, "message_dead_use_redrive"

            # Any still-active older attempt is superseded by this new one.
            if st["current_delivery_id"]:
                self.db.execute(
                    "update deliveries set status='retried',"
                    " failure_class='superseded', reason='superseded_by_new_delivery',"
                    " settled_at=? where delivery_id=? and status in"
                    " ('pending','inflight')",
                    (_now(), st["current_delivery_id"]),
                )
            # A duplicate delivery of an already-acked message must not demote
            # the durable state; it will be settled back to acked by replay.
            if st["state"] == "acked":
                new_state = "acked"
                episode = st["episode_attempts"] + 1
            elif trigger == "redrive" and st["state"] == "dead":
                # A manual redrive starts a fresh retry budget.
                new_state = "inflight"
                episode = 1
            else:
                new_state = "inflight"
                episode = st["episode_attempts"] + 1
            self.db.execute(
                "insert into deliveries(delivery_id, origin_id, revision, attempt_no,"
                " trigger, status, created_at) values(?,?,?,?,?,'inflight',?)",
                (did, origin_id, revision, st["attempt_count"] + 1, trigger, _now()),
            )
            self.db.execute(
                "update message_state set state=?, current_delivery_id=?,"
                " attempt_count=attempt_count+1, episode_attempts=?,"
                " updated_at=? where origin_id=? and revision=?",
                (new_state, did, episode, _now(), origin_id, revision),
            )
            self.db.execute("commit")
        except sqlite3.OperationalError as exc:
            if self.db.in_transaction:
                self.db.execute("rollback")
            if _is_busy(exc):
                return None, "storage_busy"
            raise
        return did, None

    def deliver(self, delivery_id, on_after_apply=None):
        """Execute one delivery attempt and settle it against this delivery_id.

        The loader writes the fact and the effect receipt in one transaction.
        Only after that does settlement run.  A process failure in between
        leaves the delivery inflight with a durable receipt; ``recover()``
        reads that receipt before deciding to resend or acknowledge.
        """
        d = self.delivery(delivery_id)
        if d is None:
            return {"status": "ignored", "reason": "unknown_delivery",
                    "delivery_id": delivery_id}
        if d["status"] not in ("pending", "inflight"):
            return {"status": "ignored",
                    "reason": "delivery_already_" + d["status"],
                    "delivery_id": delivery_id}

        st = self.state(d["origin_id"], d["revision"])
        if st is None:
            return {"status": "ignored", "reason": "unknown_message"}
        if st["state"] == "dead":
            return {"status": "ignored", "reason": "message_dead",
                    "delivery_id": delivery_id}
        if st["current_delivery_id"] not in (delivery_id, None):
            return {"status": "ignored", "reason": "superseded_delivery",
                    "delivery_id": delivery_id}

        msg = self.message(d["origin_id"], d["revision"])
        try:
            outcome = self.store.apply(msg["body"], delivery_id=delivery_id)
        except RetryableBusyError as exc:
            outcome = {"status": "retryable", "reason": "storage_busy",
                       "error": str(exc)}
        except Exception as exc:  # unknown/ambiguous: never a business rejection
            outcome = {"status": "retryable", "reason": "ambiguous_error",
                       "error": type(exc).__name__ + ": " + str(exc)}

        if outcome["status"] in ("applied", "replayed"):
            if on_after_apply is not None:
                on_after_apply(delivery_id, outcome)
        return self.settle(delivery_id, outcome)

    def settle(self, delivery_id, outcome):
        """Bind a settlement to one delivery; late settlements modify nothing."""
        try:
            self.db.execute("begin immediate")
            d = self.delivery(delivery_id)
            if d is None:
                self.db.execute("rollback")
                return {"status": "ignored", "reason": "unknown_delivery",
                        "delivery_id": delivery_id}
            if d["status"] not in ("pending", "inflight"):
                self.db.execute("rollback")
                return {"status": "ignored",
                        "reason": "delivery_already_" + d["status"],
                        "delivery_id": delivery_id}

            st = self.state(d["origin_id"], d["revision"])
            if st is None:
                self.db.execute("rollback")
                return {"status": "ignored", "reason": "unknown_message"}

            result_json = json.dumps(outcome, sort_keys=True)
            if st["state"] == "acked" and st["current_delivery_id"] not in (
                delivery_id,
                None,
            ):
                # A newer attempt owns the state.
                self._mark_delivery(delivery_id, "retried", "superseded",
                                    "superseded_delivery", outcome)
                self.db.execute("commit")
                return {"status": "ignored", "reason": "superseded_delivery",
                        "delivery_id": delivery_id}

            status = outcome.get("status")
            if status in ("applied", "replayed"):
                self._mark_delivery(delivery_id, "acked", None, None, outcome)
                self.db.execute(
                    "update message_state set state='acked',"
                    " current_delivery_id=null, episode_attempts=0,"
                    " failure_class=null, reason=?, result_json=?, updated_at=?"
                    " where origin_id=? and revision=?",
                    (outcome.get("reason"), result_json, _now(),
                     d["origin_id"], d["revision"]),
                )
                final = "acked"
            elif status in ("permanent", "stale"):
                failure = "permanent" if status == "permanent" else "stale"
                self._mark_delivery(delivery_id, "dead", failure,
                                    outcome.get("reason"), outcome)
                self.db.execute(
                    "update message_state set state='dead', current_delivery_id=null,"
                    " failure_class=?, reason=?, result_json=?, updated_at=?"
                    " where origin_id=? and revision=?",
                    (failure, outcome.get("reason"), result_json, _now(),
                     d["origin_id"], d["revision"]),
                )
                final = "dead"
            else:
                # Retryable (explicit busy or an ambiguous/unknown error).
                failure = "retryable"
                if (st["episode_attempts"] < self.max_attempts
                        and st["attempt_count"] < MAX_DELIVERIES):
                    self._mark_delivery(delivery_id, "retried", failure,
                                        outcome.get("reason"), outcome)
                    self.db.execute(
                        "update message_state set state='pending',"
                        " current_delivery_id=null, failure_class=?, reason=?,"
                        " result_json=?, updated_at=? where origin_id=? and revision=?",
                        (failure, outcome.get("reason"), result_json, _now(),
                         d["origin_id"], d["revision"]),
                    )
                    final = "pending"
                else:
                    self._mark_delivery(delivery_id, "dead", failure,
                                        "retry_exhausted", outcome)
                    self.db.execute(
                        "update message_state set state='dead',"
                        " current_delivery_id=null, failure_class=?, reason=?,"
                        " result_json=?, updated_at=? where origin_id=? and revision=?",
                        (failure, "retry_exhausted", result_json, _now(),
                         d["origin_id"], d["revision"]),
                    )
                    final = "dead"
            self.db.execute("commit")
            settled = self.delivery(delivery_id)
            settled["message_state"] = self.state(d["origin_id"], d["revision"])
            settled["final_state"] = final
            return settled
        except sqlite3.OperationalError as exc:
            if self.db.in_transaction:
                self.db.execute("rollback")
            if _is_busy(exc):
                return {"status": "retryable", "reason": "storage_busy",
                        "delivery_id": delivery_id}
            raise

    def _mark_delivery(self, delivery_id, status, failure_class, reason, result):
        self.db.execute(
            "update deliveries set status=?, failure_class=?, reason=?,"
            " result_json=?, settled_at=? where delivery_id=? and status in"
            " ('pending','inflight')",
            (status, failure_class, reason, json.dumps(result, sort_keys=True),
             _now(), delivery_id),
        )

    def settle_outcome(self, delivery_id, outcome):
        """Public alias used to inject a late settlement in exercises/self-tests."""
        return self.settle(delivery_id, outcome)

    # ------------------------------------------------------------------
    # retry / recovery
    # ------------------------------------------------------------------
    def process_pending(self):
        """Make a fresh attempt for every message whose retry is still allowed."""
        ids = [
            (r[0], r[1])
            for r in self.db.execute(
                "select origin_id, revision from message_state where state='pending'"
                " order by updated_at, origin_id, revision"
            ).fetchall()
        ]
        results = []
        for origin_id, revision in ids:
            st = self.state(origin_id, revision)
            if st is None or st["state"] != "pending":
                continue
            if (self.max_attempts is not None
                    and st["episode_attempts"] >= self.max_attempts):
                continue
            did, err = self._create_delivery(origin_id, revision, "retry")
            if did is None:
                results.append({"origin_id": origin_id, "revision": revision,
                                "status": "blocked", "reason": err})
                continue
            results.append(self.deliver(did))
        return results

    def timeout_resend(self, delivery_id):
        """Simulate a timeout: supersede an in-doubt attempt with a new one."""
        d = self.delivery(delivery_id)
        if d is None:
            return {"status": "ignored", "reason": "unknown_delivery"}
        if d["status"] not in ("pending", "inflight"):
            return {"status": "ignored",
                    "reason": "delivery_already_" + d["status"]}
        new_id, err = self._create_delivery(
            d["origin_id"], d["revision"], "timeout_resend"
        )
        if new_id is None:
            return {"status": "rejected", "reason": err}
        return {"status": "resent", "old_delivery_id": delivery_id,
                "delivery_id": new_id}

    def recover(self):
        """Resolve in-doubt attempts by reading durable effects first."""
        rows = self.db.execute(
            "select delivery_id, origin_id, revision from deliveries"
            " where status in ('pending','inflight') order by created_at, rowid"
        ).fetchall()
        actions = []
        for delivery_id, origin_id, revision in rows:
            d = self.delivery(delivery_id)
            if d is None or d["status"] not in ("pending", "inflight"):
                continue
            st = self.state(origin_id, revision)
            if st is None:
                continue
            if st["current_delivery_id"] != delivery_id:
                # A newer delivery owns this identity; do not settle it here.
                try:
                    self.db.execute("begin immediate")
                    self._mark_delivery(delivery_id, "retried", "superseded",
                                        "superseded_delivery", {})
                    self.db.execute("commit")
                except sqlite3.OperationalError:
                    if self.db.in_transaction:
                        self.db.execute("rollback")
                    raise
                actions.append({"delivery_id": delivery_id,
                                "action": "superseded"})
                continue

            receipt = self.store.receipt(origin_id, revision)
            if receipt is not None:
                # The effect is durable: acknowledge, never execute again.
                settled = self.settle(
                    delivery_id, self.store.replayed_result(receipt)
                )
                actions.append({"delivery_id": delivery_id,
                                "action": "ack_from_receipt",
                                "result": settled.get("final_state")})
                continue

            # No receipt: the transaction did not commit (or never ran), so a
            # new attempt is safe.  The old attempt becomes an ambiguous
            # retry, never a permanent failure.
            new_id, err = self._create_delivery(
                origin_id, revision, "recovery", None
            )
            if new_id is None:
                actions.append({"delivery_id": delivery_id,
                                "action": "blocked", "reason": err})
                continue
            settled = self.deliver(new_id)
            actions.append({"delivery_id": delivery_id,
                            "action": "resent", "new_delivery_id": new_id,
                            "result": settled.get("final_state")})
        return actions

    # ------------------------------------------------------------------
    # redrive
    # ------------------------------------------------------------------
    def redrive(self, origin_id, revision, choice="original",
                corrected=None, delivery_id=None):
        """Redrive a dead message, either verbatim or as a new revision.

        ``original`` keeps the identity and body untouched.  ``corrected`` is
        only legal when the origin has no success receipt yet; it creates the
        next revision with a complete corrected body, keeps the predecessor
        link and requires an explicit new ``expected_row_version``.  The
        service never reads the latest row version to fill that in.
        """
        if choice not in ("original", "corrected"):
            raise InvalidRedrive("choice must be 'original' or 'corrected'")
        st = self.state(origin_id, revision)
        if st is None:
            raise InvalidRedrive("unknown message")

        if choice == "original":
            if st["state"] == "acked":
                return {"status": "ignored", "reason": "original_already_applied",
                        "origin_id": origin_id, "revision": revision}
            if st["state"] != "dead":
                return {"status": "ignored", "reason": "message_not_dead",
                        "origin_id": origin_id, "revision": revision}
                msg = self.message(origin_id, revision)
                try:
                    outcome = self.store.apply(msg["body"], delivery_id=did)
                except RetryableBusyError as exc:
                    outcome = {"status": "retryable", "reason": "storage_busy",
                               "error": str(exc)}
                except Exception as exc:  # unknown: never a business rejection
                    outcome = {"status": "retryable", "reason": "ambiguous_error",
                               "error": type(exc).__name__ + ": " + str(exc)}
                # A late settlement belongs to this delivery_id only and can
                # never reach a newer attempt for the same message identity.
                settled = self.settle(did, outcome)
                settled["redrive_choice"] = "original"
                return settled
            else:
                did, err = self._create_delivery(
                    origin_id, revision, "redrive", delivery_id
                )
                if did is None:
                    return {"status": "rejected", "reason": err,
                            "origin_id": origin_id, "revision": revision}
                result = self.deliver(did)
                result["redrive_choice"] = "original"
                return result

        # corrected redrive
        if self.effect_count(origin_id) > 0:
            raise InvalidRedrive(
                "cannot apply a correction over an existing success receipt"
            )
        if corrected is None or not isinstance(corrected, dict):
            raise InvalidRedrive("corrected body is required")
        if "key" not in corrected or "delta" not in corrected:
            raise InvalidRedrive("corrected body must be complete (key, delta)")
        if "expected_row_version" not in corrected:
            raise InvalidRedrive(
                "administrator must explicitly supply expected_row_version"
            )
        exp = corrected["expected_row_version"]
        if type(exp) is not int or exp < 0:
            raise InvalidRedrive("expected_row_version must be a non-negative int")
        if "origin_id" in corrected and corrected["origin_id"] != origin_id:
            raise InvalidRedrive("corrected origin_id must match")

        latest = self.db.execute(
            "select max(revision) from messages where origin_id=?", (origin_id,)
        ).fetchone()[0]
        if revision != latest:
            raise InvalidRedrive("corrections must extend the latest revision")
        next_revision = latest + 1
        if next_revision > MAX_REVISIONS:
            raise InvalidRedrive("revision limit reached")

        body = {
            "origin_id": origin_id,
            "revision": next_revision,
            "key": corrected["key"],
            "delta": corrected["delta"],
            "expected_row_version": exp,
        }
        bad = validation_error(body)
        if bad is not None:
            return {"status": "permanent", "reason": bad,
                    "origin_id": origin_id, "revision": next_revision}
        canonical = canonical_body(body)

        try:
            self.db.execute("begin immediate")
            # Re-check the no-receipt rule under the write lock.
            if self.db.execute(
                "select count(*) from effects where origin_id=?", (origin_id,)
            ).fetchone()[0] > 0:
                self.db.execute("rollback")
                raise InvalidRedrive(
                    "cannot apply a correction over an existing success receipt"
                )
            self.db.execute(
                "insert into messages(origin_id, revision, key, delta_json,"
                " expected_row_version, body_json, predecessor_origin_id,"
                " predecessor_revision, is_correction, created_at)"
                " values(?,?,?,?,?,?,?,?,1,?)",
                (
                    origin_id,
                    next_revision,
                    body["key"],
                    json.dumps(body["delta"]),
                    exp,
                    canonical,
                    origin_id,
                    revision,
                    _now(),
                ),
            )
            self.db.execute(
                "insert into message_state(origin_id, revision, state,"
                " attempt_count, updated_at) values(?,?,'pending',0,?)",
                (origin_id, next_revision, _now()),
            )
            self.db.execute("commit")
        except sqlite3.OperationalError as exc:
            if self.db.in_transaction:
                self.db.execute("rollback")
            if _is_busy(exc):
                return {"status": "retryable", "reason": "storage_busy"}
            raise

        did, err = self._create_delivery(
            origin_id, next_revision, "redrive", delivery_id
        )
        if did is None:
            return {"status": "rejected", "reason": err,
                    "origin_id": origin_id, "revision": next_revision}
        result = self.deliver(did)
        result["revision"] = next_revision
        return result


class Delivery:
    """Compatibility one-shot sender used by the starting demo.

    For durable multi-attempt behaviour use ``DeadLetterService``; this wrapper
    still keeps an in-memory result list for the original example.
    """

    def __init__(self, loader):
        self.loader = loader
        self.results = []

    def send(self, event):
        result = self.loader(event)
        self.results.append(result)
        return result


__all__ = [
    "DeadLetterService",
    "Delivery",
    "IdentityBodyConflict",
    "InvalidRedrive",
    "RetryableBusyError",
    "MAX_DELIVERIES",
    "MAX_REVISIONS",
    "DEFAULT_MAX_ATTEMPTS",
]
