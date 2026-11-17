"""Event loader: apply one business message to a facts row exactly once.

Design notes
------------
* Facts, the immutable message inbox, the per-message success receipt and the
  delivery ledger all live in the *same* SQLite database.  The fact update and
  the receipt (effect) are written in one ``BEGIN IMMEDIATE`` transaction.
* Therefore the durable receipt is the authority for an unknown ACK: if the
  receipt exists the effect was committed, otherwise the transaction did not
  commit and a redelivery is safe.  The delivery ledger is never used to decide
  whether arithmetic should run again.
* A message is identified by ``(origin_id, revision)``.  The body stored for an
  identity is immutable and a resend/redrive with the same identity must have
  byte-identical business fields.
"""

import json
import sqlite3

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

BUSINESS_FIELDS = ("origin_id", "revision", "key", "expected_row_version", "delta")


class RetryableBusyError(Exception):
    """A controlled, retryable storage/busy fault.  No effect was written."""


SCHEMA = """
create table if not exists facts(
    key text primary key,
    value integer not null,
    version integer not null
);

-- Immutable inbox.  A corrected redrive inserts the next revision; existing
-- message rows are never rewritten.
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

-- Current workflow state for one message identity.
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

-- One row per delivery attempt.  delivery_id is independent of message
-- identity; timeout resend and dead-letter redrive add rows.
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

-- Success effect and original-message receipt.  One row per applied identity.
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


def open_store(path):
    """Open the single SQLite store used for facts, inbox and delivery ledger."""
    db = sqlite3.connect(path, isolation_level=None)
    db.execute("pragma busy_timeout=5000")
    db.execute("pragma foreign_keys=on")
    db.execute("pragma journal_mode=wal")
    ensure_schema(db)
    return db


def ensure_schema(db):
    db.executescript(SCHEMA)
    return db


def _business_fields(body):
    return {name: body.get(name) for name in BUSINESS_FIELDS}


def canonical_body(body):
    """Canonical representation used for same-identity/same-body checks."""
    fields = _business_fields(body)
    return json.dumps(
        fields,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=repr,
    )


def validation_error(fields):
    """Return a permanent-error reason for illegal types, else None.

    Type validation deliberately precedes row-version checks: an illegal body
    is a permanent protocol error regardless of the current fact version.
    """
    origin_id = fields.get("origin_id")
    revision = fields.get("revision")
    key = fields.get("key")
    delta = fields.get("delta")
    expected = fields.get("expected_row_version")

    if not isinstance(origin_id, str) or not origin_id:
        return "invalid_origin_id"
    if type(revision) is not int or revision < 1:
        return "invalid_revision"
    if not isinstance(key, str) or key == "":
        return "invalid_key"
    if type(delta) is not int:  # bool is an int subclass; type() excludes it
        return "invalid_delta_type"
    if delta < INT64_MIN or delta > INT64_MAX:
        return "invalid_delta_range"
    if type(expected) is not int or expected < 0:
        return "invalid_expected_row_version"
    return None


def _rollback(db):
    if db.in_transaction:
        db.execute("rollback")


def _signed_result(value, delta):
    result = value + delta
    if result < INT64_MIN or result > INT64_MAX:
        return None
    return result


class EventLoader:
    """Transactional, idempotent loader for one message identity."""

    def __init__(self, db):
        self.db = db

    # -- receipts ---------------------------------------------------------
    def receipt(self, origin_id, revision):
        row = self.db.execute(
            "select origin_id, revision, key, value_before, version_before,"
            " value_after, version_after, first_delivery_id, result_json"
            " from effects where origin_id=? and revision=?",
            (origin_id, revision),
        ).fetchone()
        if row is None:
            return None
        return {
            "origin_id": row[0],
            "revision": row[1],
            "key": row[2],
            "value_before": row[3],
            "version_before": row[4],
            "value_after": row[5],
            "version_after": row[6],
            "first_delivery_id": row[7],
            "result": json.loads(row[8]),
        }

    def replayed_result(self, receipt):
        stored = receipt["result"]
        return {
            "status": "replayed",
            "origin_id": receipt["origin_id"],
            "revision": receipt["revision"],
            "key": receipt["key"],
            "value": receipt["value_after"],
            "version": receipt["version_after"],
            "previous_value": receipt["value_before"],
            "previous_version": receipt["version_before"],
            "first_status": stored.get("status", "applied"),
            "first_delivery_id": receipt["first_delivery_id"],
            "reason": "message_identity_already_applied",
        }

    # -- apply ------------------------------------------------------------
    def __call__(self, body, delivery_id=None):
        return self.apply(body, delivery_id=delivery_id)

    def apply(self, body, delivery_id=None):
        if not isinstance(body, dict):
            return {
                "status": "permanent",
                "reason": "invalid_body_type",
                "origin_id": None,
                "revision": None,
                "key": None,
            }
        fields = _business_fields(body)
        bad = validation_error(fields)
        if bad is not None:
            return self._permanent(fields, bad)

        origin_id = fields["origin_id"]
        revision = fields["revision"]
        key = fields["key"]

        # Fast replay path: success is durable and is not re-evaluated against
        # a fact row that may have changed since the original application.
        existing = self.receipt(origin_id, revision)
        if existing is not None:
            return self.replayed_result(existing)

        try:
            self.db.execute("begin immediate")
        except sqlite3.OperationalError as exc:
            if _is_busy(exc):
                raise RetryableBusyError(str(exc)) from exc
            raise

        try:
            # Re-check under the write lock (another process may have won).
            existing = self.receipt(origin_id, revision)
            if existing is not None:
                self.db.execute("rollback")
                return self.replayed_result(existing)

            # The identity's body is immutable: reject a same-identity resend
            # that carries different business fields.
            inbox = self.db.execute(
                "select body_json from messages where origin_id=? and revision=?",
                (origin_id, revision),
            ).fetchone()
            if inbox is not None and inbox[0] != canonical_body(body):
                self.db.execute("rollback")
                return self._permanent(fields, "identity_body_conflict")

            row = self.db.execute(
                "select value, version from facts where key=?", (key,)
            ).fetchone()
            value_before, version_before = row if row is not None else (0, 0)
            expected = fields["expected_row_version"]

            if expected != version_before:
                self.db.execute("rollback")
                return {
                    "status": "stale",
                    "origin_id": origin_id,
                    "revision": revision,
                    "key": key,
                    "expected_row_version": expected,
                    "current_row_version": version_before,
                    "reason": "row_version_mismatch",
                }

            value_after = _signed_result(value_before, fields["delta"])
            if value_after is None:
                self.db.execute("rollback")
                return self._permanent(fields, "int64_overflow")

            version_after = version_before + 1
            result = {
                "status": "applied",
                "origin_id": origin_id,
                "revision": revision,
                "key": key,
                "value": value_after,
                "version": version_after,
                "previous_value": value_before,
                "previous_version": version_before,
                "delivery_id": delivery_id,
            }
            self.db.execute(
                "insert into facts(key, value, version) values(?,?,?)"
                " on conflict(key) do update set value=excluded.value,"
                " version=excluded.version",
                (key, value_after, version_after),
            )
            # Effect and original-message receipt, same transaction as facts.
            self.db.execute(
                "insert into effects(origin_id, revision, key, value_before,"
                " version_before, value_after, version_after, first_delivery_id,"
                " result_json, created_at) values(?,?,?,?,?,?,?,?,?,datetime('now'))",
                (
                    origin_id,
                    revision,
                    key,
                    value_before,
                    version_before,
                    value_after,
                    version_after,
                    delivery_id,
                    json.dumps(result, sort_keys=True),
                ),
            )
            self.db.execute("commit")
            return result
        except sqlite3.IntegrityError:
            _rollback(self.db)
            existing = self.receipt(origin_id, revision)
            if existing is not None:
                return self.replayed_result(existing)
            raise
        except sqlite3.OperationalError as exc:
            _rollback(self.db)
            if _is_busy(exc):
                raise RetryableBusyError(str(exc)) from exc
            raise
        except BaseException:
            _rollback(self.db)
            raise

    @staticmethod
    def _permanent(fields, reason):
        return {
            "status": "permanent",
            "origin_id": fields.get("origin_id"),
            "revision": fields.get("revision"),
            "key": fields.get("key"),
            "reason": reason,
        }


def _is_busy(exc):
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def add_once(db, event):
    """Backwards-compatible single-submission helper from the starting material."""
    return EventLoader(db).apply(event)
