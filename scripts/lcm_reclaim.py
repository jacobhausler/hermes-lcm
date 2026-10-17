#!/usr/bin/env python3
"""LCM retention reclaim — prune raw message rows and shrink the database.

Cron-usable reclaim verb for the append-heavy raw corpus (est-cjd6p policy):
7-day TTL OR 1 GiB live-bytes size cap, whichever bites first, protected set
of warm + explicitly-listed sessions, whole-cold-session eviction oldest-first
under the cap, verified online backup before any delete (skipped with a
documented reason when the backup is not cheap — free space under 2.5x the db
size), chunk soft-archive ahead of each delete batch, WAL TRUNCATE checkpoint
+ VACUUM afterwards.

Shrink note: production lcm.db runs auto_vacuum=none, so PRAGMA
incremental_vacuum is NOT available in place (enabling it needs a full VACUUM
anyway); the VACUUM here runs under busy_timeout so estate writers interleave,
but it rewrites the whole file — schedule the verb off-peak, not under
nightly-maintain fan-out.

Dry-run is the default (``--dry-run`` is the explicit spelling); pass
``--apply`` to delete. Dry-run opens the database read-only. A full nightly
cadence:

    python3 scripts/lcm_reclaim.py --apply --quiet --json >> /path/keeper.log

The last emitted ``lcm-reclaim`` line is single-line greppable for monitors.
``busy_timeout`` plus batched deletes keep live writers interleaved; prefer
a low-load window because the final VACUUM rewrites the whole file.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from retention import (  # noqa: E402
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_DB_BYTES,
    DEFAULT_PROTECT_HOURS,
    DEFAULT_TTL_DAYS,
    apply_reclaim,
    checkpoint_wal,
    online_backup,
    plan_reclaim,
    plan_store_ids,
    vacuum_database,
)

# online_backup() writes a full db copy next to the source; treat a backup
# as "cheap" only when there is generous headroom beyond the copy itself.
_BACKUP_CHEAP_FACTOR = 2.5


def _parse_size(raw: str) -> int:
    text = str(raw).strip().upper().replace("_", "").replace(",", "")
    multiplier = 1
    for suffix, factor in (("GIB", 1024 ** 3), ("MIB", 1024 ** 2), ("KIB", 1024)):
        if text.endswith(suffix):
            multiplier, text = factor, text[: -len(suffix)]
            break
    value = int(float(text) * multiplier)
    if value <= 0:
        raise argparse.ArgumentTypeError("size must be positive")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lcm_reclaim",
        description="LCM raw-row retention reclaim (dry-run unless --apply).",
    )
    parser.add_argument(
        "--db",
        default="/home/hermes/.hermes/lcm.db",
        help="path to lcm.db (default: %(default)s)",
    )
    parser.add_argument(
        "--ttl-days", type=float, default=DEFAULT_TTL_DAYS,
        help="raw-row TTL in days (default: %(default)s)",
    )
    parser.add_argument(
        "--max-db-bytes", type=_parse_size, default=DEFAULT_MAX_DB_BYTES,
        help="live-bytes size cap, e.g. 1GiB / 800MiB (default: 1GiB)",
    )
    parser.add_argument(
        "--protect-hours", type=float, default=DEFAULT_PROTECT_HOURS,
        help="sessions warm within this window are never pruned (default: %(default)s)",
    )
    parser.add_argument(
        "--protect-session", action="append", default=[],
        help="session_id to protect in addition to warm sessions (repeatable)",
    )
    parser.add_argument(
        "--batch", type=int, default=DEFAULT_BATCH_SIZE,
        help="delete batch size (default: %(default)s)",
    )
    parser.add_argument(
        "--busy-timeout-ms", type=int, default=60000,
        help="SQLite busy timeout for writers (default: %(default)s)",
    )
    parser.add_argument(
        "--backup-dir", default=None,
        help="directory for the pre-reclaim backup (default: beside the db)",
    )
    parser.add_argument(
        "--no-backup", action="store_true",
        help="skip the pre-reclaim backup even when it would be cheap (not advised)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="explicit read-only plan mode (the default; refuses --apply)",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="actually delete; without this flag the verb is read-only",
    )
    parser.add_argument(
        "--no-vacuum", action="store_true",
        help="skip the final WAL checkpoint + VACUUM shrink step",
    )
    parser.add_argument(
        "--no-quick-check", action="store_true",
        help="skip the post-apply timed quick_check(1) report",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="emit a machine-readable JSON summary line",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="only emit the final lcm-reclaim summary line",
    )
    args = parser.parse_args(argv)

    if args.apply and args.dry_run:
        parser.error("--apply and --dry-run are mutually exclusive")
    apply_mode = bool(args.apply)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"lcm-reclaim result=error error=database not found: {db_path}", file=sys.stderr)
        return 2

    # Writer-safety for the plan pass: dry-run never mutates, so open the
    # database strictly read-only (mode=ro) — no -wal/-shm side effects on a
    # writer-hot estate db.
    if apply_mode:
        conn = sqlite3.connect(str(db_path), timeout=max(1.0, args.busy_timeout_ms / 1000.0))
    else:
        conn = sqlite3.connect(
            f"file:{db_path}?mode=ro", uri=True, timeout=max(1.0, args.busy_timeout_ms / 1000.0)
        )
    conn.isolation_level = None
    try:
        conn.execute(f"PRAGMA busy_timeout={int(args.busy_timeout_ms)}")

        def emit(msg: str) -> None:
            if not args.quiet:
                print(msg, flush=True)

        started = time.monotonic()
        plan = plan_reclaim(
            conn,
            ttl_days=args.ttl_days,
            max_db_bytes=args.max_db_bytes,
            protect_hours=args.protect_hours,
            protect_sessions=args.protect_session,
        )
        victims = plan_store_ids(plan)
        size_before = int(db_path.stat().st_size)
        emit(
            f"plan: ttl_days={args.ttl_days:g} cap={args.max_db_bytes} "
            f"ttl_rows={len(plan['ttl_store_ids'])} "
            f"evicted_sessions={len(plan['evicted_sessions'])} "
            f"victims={len(victims)} protected={len(plan['protected_sessions'])} "
            f"live_bytes={plan['live_bytes_before']} "
            f"projected_live_bytes={plan['projected_live_bytes']}"
        )

        summary: dict[str, object] = {
            "db": str(db_path),
            "apply": apply_mode,
            "ttl_days": args.ttl_days,
            "max_db_bytes": args.max_db_bytes,
            "victims": len(victims),
            "ttl_rows": len(plan["ttl_store_ids"]),
            "evicted_sessions": len(plan["evicted_sessions"]),
            "protected_sessions": len(plan["protected_sessions"]),
            "live_bytes_before": plan["live_bytes_before"],
            "projected_live_bytes": plan["projected_live_bytes"],
            "bytes_before": size_before,
            "deleted_messages": 0,
            "archived_chunks": 0,
            "backup": None,
            "backup_skip": None,
            "vacuumed": False,
            "bytes_after": size_before,
            "quick_check": None,
            "quick_check_s": None,
        }

        if not apply_mode:
            summary["result"] = "dry-run"
        else:
            # Archive-before-delete: verified online backup, lcmpurge-style.
            # Cheapness gate — a backup is only taken when the target volume
            # has generous room for a full copy; otherwise it is skipped with
            # the reason recorded (documented skip, never silent).
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            backup_dir = Path(args.backup_dir) if args.backup_dir else db_path.parent
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / f"{db_path.name}.pre-lcmpurge-{stamp}Z.bak"
            need = int(size_before * _BACKUP_CHEAP_FACTOR)
            free = int(shutil.disk_usage(str(backup_dir)).free)
            if args.no_backup:
                summary["backup_skip"] = "skipped by --no-backup"
                emit("backup: skipped by --no-backup (deletes unprotected by snapshot)")
            elif free < need:
                summary["backup_skip"] = (
                    f"skipped: not cheap — {free} bytes free < {_BACKUP_CHEAP_FACTOR}x db size ({need})"
                )
                emit(f"backup: SKIPPED (not cheap) — {summary['backup_skip']}")
            else:
                online_backup(conn, str(backup_path))
                summary["backup"] = str(backup_path)
                emit(f"backup verified: {backup_path}")

            if victims:
                counters = apply_reclaim(conn, plan, batch_size=args.batch)
                summary["deleted_messages"] = int(counters["deleted_messages"])
                summary["archived_chunks"] = int(counters["archived_chunks"])
                emit(
                    f"deleted {counters['deleted_messages']} raw rows in "
                    f"{counters['batches']} batches "
                    f"(chunks archived: {counters['archived_chunks']})"
                )
            if not args.no_vacuum:
                summary["vacuumed"] = bool(vacuum_database(conn))
                emit(f"vacuum: {'ok' if summary['vacuumed'] else 'skipped'}")
            else:
                checkpoint_wal(conn)
            summary["bytes_after"] = int(db_path.stat().st_size)
            if not args.no_quick_check:
                qc_start = time.monotonic()
                check = conn.execute("PRAGMA quick_check(1)").fetchone()
                qc_elapsed = time.monotonic() - qc_start
                summary["quick_check"] = str(check[0]) if check else None
                summary["quick_check_s"] = round(qc_elapsed, 1)
                emit(
                    f"quick_check: {summary['quick_check']} t={summary['quick_check_s']}s "
                    f"db={summary['bytes_after']} bytes"
                )
            summary["result"] = "ok"
    finally:
        conn.close()

    summary["elapsed_s"] = round(time.monotonic() - started, 1)
    freed = int(summary.get("bytes_before") or 0) - int(summary.get("bytes_after") or 0)
    line = (
        f"lcm-reclaim result={summary['result']} deleted={summary['deleted_messages']} "
        f"bytes={summary['bytes_before']}->{summary['bytes_after']} "
        f"freed={freed} "
        f"qc={summary['quick_check']} qc_t={summary['quick_check_s']}s "
        f"elapsed={summary['elapsed_s']}s"
    )
    print(line, flush=True)
    if args.json:
        print(json.dumps(summary), flush=True)
    return 0 if summary["result"] in ("ok", "dry-run") else 2


if __name__ == "__main__":
    raise SystemExit(main())
