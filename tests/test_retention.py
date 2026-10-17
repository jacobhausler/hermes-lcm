"""Focused tests for the opt-in retention reclaim module (est-cjd6p).

Every test builds a THROWAWAY temp database mirroring the live schema
(messages + external-content FTS shadow + sync triggers); the live db is
never touched.
"""

import time

from hermes_lcm import retention

_SCHEMA = """
CREATE TABLE messages (
                store_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                source TEXT DEFAULT '',
                conversation_id TEXT DEFAULT '',
                role TEXT NOT NULL,
                content TEXT,
                tool_call_id TEXT,
                tool_calls TEXT,
                tool_name TEXT,
                timestamp REAL NOT NULL,
                token_estimate INTEGER DEFAULT 0,
                pinned INTEGER DEFAULT 0,
                ingested_at REAL,
                observed_at REAL,
                observed_at_source TEXT
            );
CREATE INDEX idx_msg_session ON messages(session_id, store_id);
CREATE INDEX idx_msg_session_ts ON messages(session_id, timestamp);
CREATE VIRTUAL TABLE messages_fts USING fts5(
                content, content='messages', content_rowid='store_id'
            );
CREATE TRIGGER msg_fts_insert AFTER INSERT ON messages BEGIN
                INSERT INTO messages_fts(rowid, content)
                    VALUES (new.store_id, new.content);
            END;
CREATE TRIGGER msg_fts_delete AFTER DELETE ON messages BEGIN
                INSERT INTO messages_fts(messages_fts, rowid, content)
                    VALUES('delete', old.store_id, old.content);
            END;
"""


def _make_db(tmp_path, name="lcm_test.db"):
    import sqlite3

    conn = sqlite3.connect(str(tmp_path / name))
    conn.executescript(_SCHEMA)
    return conn


def _seed(conn, session_id, *, age_days, rows, text="payload"):
    now = time.time()
    for i in range(rows):
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp)"
            " VALUES (?, 'user', ?, ?)",
            (session_id, f"{text}-{session_id}-{i}", now - age_days * 86400.0 + i),
        )
    conn.commit()


def _count(conn):
    return int(conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0])


def test_missing_messages_table_yields_empty_plan(tmp_path):
    import sqlite3

    conn = sqlite3.connect(str(tmp_path / "empty.db"))
    plan = retention.plan_reclaim(conn)
    assert plan["ttl_store_ids"] == []
    assert plan["evict_store_ids"] == []
    assert retention.plan_store_ids(plan) == []
    conn.close()


def test_ttl_prunes_old_cold_rows_only(tmp_path):
    conn = _make_db(tmp_path)
    _seed(conn, "old-cold", age_days=10, rows=5, text="old")
    _seed(conn, "fresh", age_days=1, rows=5, text="new")
    _seed(conn, "warm", age_days=0, rows=5, text="warm")
    plan = retention.plan_reclaim(conn, ttl_days=7, protect_hours=24.0)
    assert plan["ttl_store_ids"], "old rows should be victims"
    victims = set(retention.plan_store_ids(plan))
    old_ids = {
        int(r[0])
        for r in conn.execute(
            "SELECT store_id FROM messages WHERE session_id='old-cold'"
        ).fetchall()
    }
    kept_ids = {
        int(r[0])
        for r in conn.execute(
            "SELECT store_id FROM messages WHERE session_id IN ('fresh','warm')"
        ).fetchall()
    }
    assert victims == old_ids
    assert not (victims & kept_ids)
    assert "warm" in plan["protected_sessions"]
    conn.close()


def test_warm_and_explicit_sessions_are_protected(tmp_path):
    conn = _make_db(tmp_path)
    _seed(conn, "old-but-warm", age_days=30, rows=4, text="ancient")
    _seed(conn, "old-but-warm", age_days=0, rows=1, text="recent-ping")
    _seed(conn, "old-explicit", age_days=30, rows=4, text="ancient")
    _seed(conn, "old-gone", age_days=30, rows=4, text="ancient")
    plan = retention.plan_reclaim(
        conn, ttl_days=7, protect_hours=24.0, protect_sessions=["old-explicit"]
    )
    victims = set(retention.plan_store_ids(plan))
    warm_old = {
        int(r[0])
        for r in conn.execute(
            "SELECT store_id FROM messages WHERE session_id='old-but-warm' AND timestamp < ?",
            (time.time() - 7 * 86400,),
        ).fetchall()
    }
    gone = {
        int(r[0])
        for r in conn.execute(
            "SELECT store_id FROM messages WHERE session_id='old-gone'"
        ).fetchall()
    }
    assert not (victims & warm_old), "warm session rows are never pruned"
    explicit_old = {
        int(r[0])
        for r in conn.execute(
            "SELECT store_id FROM messages WHERE session_id='old-explicit'"
        ).fetchall()
    }
    assert not (victims & explicit_old), "explicitly protected sessions are kept"
    assert gone <= victims
    assert "old-explicit" in plan["protected_sessions"]
    conn.close()


def test_size_cap_evicts_oldest_cold_sessions_first(tmp_path):
    conn = _make_db(tmp_path)
    for idx, age in enumerate((40, 35, 30, 20)):
        _seed(conn, f"cold-{idx}", age_days=age, rows=200, text="x" * 200)
    _seed(conn, "warm", age_days=0, rows=200, text="y" * 200)
    # A tiny TTL would make every cold session a TTL session; instead force
    # the cap path with a ttl that binds nothing and a cap that binds hard.
    plan = retention.plan_reclaim(conn, ttl_days=365, max_db_bytes=1, protect_hours=24.0)
    assert plan["evicted_sessions"], "cap should evict cold sessions"
    assert "warm" not in plan["evicted_sessions"]
    # Oldest-last-activity first: cold-0 (40d) precedes cold-1 (35d) etc.
    assert plan["evicted_sessions"] == sorted(
        plan["evicted_sessions"], key=lambda sid: int(sid.split("-")[1])
    )
    conn.close()


def test_plan_is_read_only(tmp_path):
    conn = _make_db(tmp_path)
    _seed(conn, "old-cold", age_days=10, rows=10)
    before = _count(conn)
    plan = retention.plan_reclaim(conn, ttl_days=7)
    assert plan["ttl_store_ids"]
    assert _count(conn) == before, "planning must not mutate"
    conn.close()


def test_apply_deletes_and_keeps_fts_in_sync(tmp_path):
    conn = _make_db(tmp_path)
    _seed(conn, "old-cold", age_days=10, rows=10, text="pruneme")
    _seed(conn, "warm", age_days=0, rows=10, text="keepme")
    plan = retention.plan_reclaim(conn, ttl_days=7, protect_hours=24.0)
    stats = retention.apply_reclaim(conn, plan, batch_size=4)
    assert stats["deleted_messages"] == 10
    assert stats["batches"] == 3  # 10 rows / batch of 4
    assert stats["archived_chunks"] == 0  # no chunk corpus in this fixture
    assert _count(conn) == 10
    fts_hits = conn.execute(
        "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'pruneme'"
    ).fetchone()[0]
    assert int(fts_hits) == 0, "FTS shadow must follow the deletes"
    fts_kept = conn.execute(
        "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'keepme'"
    ).fetchone()[0]
    assert int(fts_kept) == 10
    conn.close()


def test_apply_empty_plan_is_a_noop(tmp_path):
    conn = _make_db(tmp_path)
    _seed(conn, "warm", age_days=0, rows=3)
    plan = retention.plan_reclaim(conn, ttl_days=7)
    stats = retention.apply_reclaim(conn, plan)
    assert stats == {"deleted_messages": 0, "batches": 0, "archived_chunks": 0}
    assert _count(conn) == 3
    conn.close()


def test_plan_store_ids_dedupes_and_sorts():
    plan = {
        "ttl_store_ids": [5, 3, 7, 3],
        "evict_store_ids": [9, 5, 1],
    }
    assert retention.plan_store_ids(plan) == [1, 3, 5, 7, 9]


def test_projected_bytes_are_ints(tmp_path):
    conn = _make_db(tmp_path)
    _seed(conn, "old-cold", age_days=10, rows=5)
    plan = retention.plan_reclaim(conn, ttl_days=7)
    assert isinstance(plan["projected_live_bytes"], int)
    assert isinstance(plan["estimated_freed_bytes"], int)
    assert plan["estimated_freed_bytes"] == max(
        0, int(plan["live_bytes_before"]) - int(plan["projected_live_bytes"])
    )
    conn.close()


def test_online_backup_produces_verified_copy(tmp_path):
    conn = _make_db(tmp_path)
    _seed(conn, "s1", age_days=1, rows=5)
    dest = str(tmp_path / "snapshot.bak")
    assert retention.online_backup(conn, dest) == dest
    import sqlite3

    snap = sqlite3.connect(dest)
    assert int(snap.execute("SELECT COUNT(*) FROM messages").fetchone()[0]) == 5
    snap.close()
    conn.close()


def test_vacuum_shrinks_after_delete(tmp_path):
    conn = _make_db(tmp_path)
    _seed(conn, "bulk", age_days=10, rows=4000, text="z" * 500)
    conn.execute("DELETE FROM messages")
    conn.commit()
    size_pre = (tmp_path / "lcm_test.db").stat().st_size
    assert retention.vacuum_database(conn) is True
    size_post = (tmp_path / "lcm_test.db").stat().st_size
    assert size_post < size_pre, "VACUUM must return pages to the OS"
    conn.close()
