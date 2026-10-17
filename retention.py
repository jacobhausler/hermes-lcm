"""Retention reclaim for the raw message corpus (est-cjd6p).

The LCM store is append-heavy: raw message rows (``messages``) plus their
external-content FTS shadow (``messages_fts``) grow monotonically until the
SQLite file itself becomes the reliability surface — integrity probes exceed
their budget and monitors go RED against a healthy database. This module is
the reclaim machinery behind ``scripts/lcm_reclaim.py`` (a cron-usable verb).

Policy defaults (owner-authorized, beads est-cjd6p):

- 7-day TTL on raw message rows OR a 1 GiB live-bytes size cap, whichever
  bites first. The TTL pass removes every raw row older than the TTL; if the
  database would still sit over the cap afterwards, whole sessions are
  evicted oldest-first until the cap is satisfied.
- Protected set: sessions warm within ``protect_hours`` plus any session
  passed explicitly. A session bound to a live run must never lose
  mid-session rows under a concurrent writer.
- Archive before delete: a verified online backup (``sqlite3`` backup API,
  lcmpurge-style ``.bak``) is taken by the CLI before anything is deleted,
  and when the chunk corpus exists the victim messages' chunks are
  soft-archived first via the plugin's existing archive concept
  (``VectorStore.archive_chunks_for_messages_on_connection``, which caps a
  batch at 256 ids — the helper here slices larger delete batches into
  compliant sub-batches) so retrieval never ranks a chunk whose raw row is
  gone.
- Deletes run in bounded batches so a writer never starves behind one giant
  DELETE, and the existing ``msg_fts_delete`` trigger keeps the FTS shadow
  in sync per batch.
- Shrink is WAL TRUNCATE checkpoint + VACUUM. Note on ``PRAGMA
  incremental_vacuum``: production lcm.db runs ``auto_vacuum=none``, so
  incremental reclaim is NOT available in place — switching to
  ``auto_vacuum=incremental`` itself requires a full VACUUM. VACUUM under
  ``busy_timeout`` with bounded batches and retries is therefore the shrink
  path, scheduled off-peak so live writers interleave around it.

Everything destructive is opt-in: this library only deletes when the caller
invokes :func:`apply_reclaim`, and the CLI is dry-run unless ``--apply``.
Summary nodes are retained (they are the bounded recall cues); the raw
lineage they point at simply ends at the TTL boundary.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Any, Iterable, Sequence

DEFAULT_TTL_DAYS = 7.0
DEFAULT_MAX_DB_BYTES = 1024 ** 3
DEFAULT_PROTECT_HOURS = 24.0
DEFAULT_BATCH_SIZE = 500

# Rough fixed row overhead beyond content length when sampling row bytes.
_ROW_OVERHEAD_ESTIMATE = 64

# Deleted-row estimate-to-committed-bytes multiplier for the projection:
# FTS shadow, docsize and indexes shrink with the table but not 1:1, so a
# 0.85 share keeps the projection conservative (evicts a bit more than the
# bare table math would).
_COMMITTED_SHARE = 0.85

# Hard cap VectorStore.archive_chunks_for_messages_on_connection enforces.
_CHUNK_ARCHIVE_BATCH = 256


def _live_bytes(conn: sqlite3.Connection) -> int:
    """Committed live bytes: allocated pages minus the freelist."""
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
    freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    return max(0, (page_count - freelist) * page_size)


def _messages_table_exists(conn: sqlite3.Connection) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages'"
        ).fetchone()
        is not None
    )


def _avg_message_row_bytes(conn: sqlite3.Connection) -> float:
    """Average committed bytes per raw message row.

    Prefers the dbstat virtual table (one quick pass); degrades to a content
    length scan when dbstat is unavailable in the linked SQLite build.
    """
    try:
        row = conn.execute(
            "SELECT SUM(pgsize) FROM dbstat WHERE name = 'messages'"
        ).fetchone()
        table_bytes = int(row[0]) if row and row[0] is not None else 0
    except sqlite3.Error:
        table_bytes = 0
    if table_bytes:
        count = int(conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] or 0)
        if count:
            return table_bytes / count
    row = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(LENGTH(COALESCE(content, ''))), 0) FROM messages"
    ).fetchone()
    count, content_bytes = int(row[0] or 0), int(row[1] or 0)
    if not count:
        return 0.0
    return (content_bytes / count) + _ROW_OVERHEAD_ESTIMATE


def _empty_plan(now: float, ttl_days: float, max_db_bytes: int) -> dict[str, Any]:
    return {
        "now": now,
        "ttl_days": ttl_days,
        "max_db_bytes": max_db_bytes,
        "cutoff": now - ttl_days * 86400.0,
        "protect_cutoff": now,
        "protected_sessions": [],
        "ttl_store_ids": [],
        "evict_store_ids": [],
        "evicted_sessions": [],
        "live_bytes_before": 0,
        "avg_row_bytes": 0.0,
        "estimated_freed_bytes": 0,
        "projected_live_bytes": 0,
    }


def plan_reclaim(
    conn: sqlite3.Connection,
    *,
    now: float | None = None,
    ttl_days: float = DEFAULT_TTL_DAYS,
    max_db_bytes: int = DEFAULT_MAX_DB_BYTES,
    protect_hours: float = DEFAULT_PROTECT_HOURS,
    protect_sessions: Iterable[str] = (),
) -> dict[str, Any]:
    """Return a delete plan; touches nothing.

    TTL pass first: raw rows older than ``ttl_days`` are victims unless their
    session stayed warm within ``protect_hours`` (a session bound to a live
    run must never lose mid-session rows under a concurrent writer). If the
    projected live size is still above ``max_db_bytes``, whole cold sessions
    are added oldest-last-activity-first until the cap projects satisfied.
    """
    now = time.time() if now is None else float(now)
    plan = _empty_plan(now, ttl_days, int(max_db_bytes))
    if not _messages_table_exists(conn):
        return plan
    cutoff = plan["cutoff"]
    protect_cutoff = now - max(0.0, float(protect_hours)) * 3600.0
    plan["protect_cutoff"] = protect_cutoff
    explicit = {str(session_id) for session_id in protect_sessions if session_id}
    warm = {
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT session_id FROM messages WHERE timestamp >= ?",
            (protect_cutoff,),
        ).fetchall()
    }
    protected = sorted(warm | explicit)
    plan["protected_sessions"] = protected
    params: list[Any] = [cutoff]
    ttl_sql = "SELECT store_id, session_id FROM messages WHERE timestamp < ?"
    if protected:
        ttl_sql += " AND session_id NOT IN (" + ",".join("?" for _ in protected) + ")"
        params.extend(protected)
    ttl_sql += " ORDER BY store_id"
    ttl_store_ids = [int(row[0]) for row in conn.execute(ttl_sql, params).fetchall()]
    plan["ttl_store_ids"] = ttl_store_ids

    live_before = int(_live_bytes(conn))
    plan["live_bytes_before"] = live_before
    avg_row = float(_avg_message_row_bytes(conn))
    plan["avg_row_bytes"] = avg_row
    victim_count = len(ttl_store_ids)

    max_db_bytes = plan["max_db_bytes"]
    projected = int(live_before - int(victim_count) * avg_row * _COMMITTED_SHARE)
    if projected > max_db_bytes:
        # Size cap still binds: evict whole cold sessions oldest-first.
        session_rows = conn.execute(
            "SELECT session_id, COUNT(*) AS message_count, MAX(timestamp) AS last_activity"
            " FROM messages GROUP BY session_id ORDER BY last_activity ASC"
        ).fetchall()
        ttl_sessions = {
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT session_id FROM messages WHERE timestamp < ?",
                (cutoff,),
            ).fetchall()
        }
        evict_sessions: list[str] = []
        evict_extra = 0
        for row in session_rows:
            if projected <= max_db_bytes:
                break
            session_id = str(row[0])
            if session_id in warm or session_id in explicit or session_id in ttl_sessions:
                continue
            evict_sessions.append(session_id)
            evict_extra += int(row[1] or 0)
            projected = int(
                live_before
                - int(victim_count + evict_extra) * avg_row * _COMMITTED_SHARE
            )
        plan["evicted_sessions"] = evict_sessions
        if evict_sessions:
            marks = ",".join("?" for _ in evict_sessions)
            plan["evict_store_ids"] = [
                int(store_id)
                for (store_id,) in conn.execute(
                    f"SELECT store_id FROM messages WHERE session_id IN ({marks}) ORDER BY store_id",
                    evict_sessions,
                ).fetchall()
            ]
            victim_count += len(plan["evict_store_ids"])
            # Evicted sessions are never TTL sessions, so the two id sets
            # cannot overlap and plan_store_ids() stays a plain union.
            projected = int(
                live_before - int(victim_count) * avg_row * _COMMITTED_SHARE
            )
    plan["projected_live_bytes"] = max(0, int(projected))
    plan["estimated_freed_bytes"] = max(
        0, int(live_before) - max(0, int(projected))
    )
    return plan


def plan_store_ids(plan: dict[str, Any]) -> list[int]:
    """Deduplicated, ordered store_id victims from a plan (ttl + eviction)."""
    seen: dict[int, None] = {}
    for store_id in list(plan.get("ttl_store_ids") or []) + list(plan.get("evict_store_ids") or []):
        seen[int(store_id)] = None
    return sorted(seen)


def _archive_chunk_batch(conn: sqlite3.Connection, batch: Sequence[int]) -> int:
    """Best-effort soft-archive of victim chunks inside the delete txn.

    ``VectorStore.archive_chunks_for_messages_on_connection`` refuses batches
    over 256 ids, so larger delete batches are sliced into compliant
    sub-batches. Returns 0 when the chunk corpus or the vector module are
    absent — reclaim never breaks on archive.
    """
    if not batch:
        return 0
    has_corpus = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table'"
            " AND name IN ('lcm_chunk_meta','lcm_embedding_profile') LIMIT 1"
        ).fetchone()
        is not None
    )
    if not has_corpus:
        return 0
    try:
        try:  # flat plugin layout (loaded as plain modules / hermes_lcm.*)
            from vector_store import VectorStore
        except ImportError:  # pragma: no cover - package-style import
            from .vector_store import VectorStore
    except Exception:  # pragma: no cover - defensive: no vector module
        return 0
    archived = 0
    for offset in range(0, len(batch), _CHUNK_ARCHIVE_BATCH):
        sub = batch[offset:offset + _CHUNK_ARCHIVE_BATCH]
        try:
            archived += int(VectorStore.archive_chunks_for_messages_on_connection(conn, sub))
        except Exception:  # pragma: no cover - defensive: archive never breaks reclaim
            return archived
    return archived


def apply_reclaim(
    conn: sqlite3.Connection,
    plan: dict[str, Any],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    archive: bool = True,
) -> dict[str, int]:
    """Delete the plan's rows in bounded batches; returns counters.

    The connection must allow writes and run with the plugin's normal trigger
    set: the ``msg_fts_delete`` trigger maintains the external-content FTS
    shadow row-by-row, so no manual FTS bookkeeping is done here. Chunk
    soft-archiving (when the corpus exists) happens in the same transaction,
    before the delete.
    """
    store_ids = plan_store_ids(plan)
    deleted = 0
    archived_chunks = 0
    batch_size = max(1, int(batch_size))
    for offset in range(0, len(store_ids), batch_size):
        batch = store_ids[offset:offset + batch_size]
        marks = ",".join("?" for _ in batch)
        if archive:
            archived_chunks += _archive_chunk_batch(conn, batch)
        conn.execute(f"DELETE FROM messages WHERE store_id IN ({marks})", batch)
        deleted += len(batch)
        conn.commit()
    return {
        "deleted_messages": deleted,
        "batches": (len(store_ids) + batch_size - 1) // batch_size if store_ids else 0,
        "archived_chunks": archived_chunks,
    }


def checkpoint_wal(conn: sqlite3.Connection) -> None:
    """Fold the WAL back into the main file, best effort."""
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass


def vacuum_database(
    conn: sqlite3.Connection,
    *,
    retries: int = 3,
    retry_sleep_s: float = 5.0,
) -> bool:
    """VACUUM with busy-retries so live writers can interleave.

    Returns True on success. ``auto_vacuum=none`` databases never shrink on
    their own; without this the reclaimed pages sit on the freelist and the
    file (and every integrity probe that walks it) stays the same size.
    ``PRAGMA incremental_vacuum`` is not an in-place alternative on such a
    db (conversion itself needs a VACUUM), so this whole-file rewrite under
    ``busy_timeout`` — run off-peak — is the shrink step.
    """
    last_error: sqlite3.Error | None = None
    for attempt in range(max(1, int(retries))):
        try:
            checkpoint_wal(conn)
            conn.execute("VACUUM")
            return True
        except sqlite3.OperationalError as exc:
            last_error = exc
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
            if attempt + 1 < max(1, int(retries)):
                time.sleep(max(0.0, float(retry_sleep_s)))
    if last_error is not None:
        raise last_error
    return False


def online_backup(source: sqlite3.Connection, dest_path: str) -> str:
    """Online-backup ``source`` to ``dest_path`` and verify before trusting it.

    Uses the sqlite3 backup API so a hot writer never yields a torn file copy,
    and quick-checks the snapshot before returning — reclaim must not delete
    rows from a database whose backup is silently corrupt.
    """
    dest = sqlite3.connect(dest_path)
    try:
        source.backup(dest)
        check = dest.execute("PRAGMA quick_check(1)").fetchone()
        if not check or str(check[0]).lower() != "ok":
            raise sqlite3.DatabaseError(f"backup quick_check failed: {check!r}")
    finally:
        dest.close()
    return dest_path
