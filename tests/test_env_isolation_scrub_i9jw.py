"""est-i9jw regression proof: a test process must never bind a database
inherited from the launching environment.

Launch the suite with an inherited LCM_DATABASE_PATH (plus fake HOME and
HERMES_HOME) and a sentinel I9JW_EXPECT_DB naming that same path. The
module-level guard in tests/conftest.py scrubs LCM_* before any plugin
submodule or test module is loaded, so:

  1. no LCM_* variable survives into test time,
  2. LCMConfig.from_env() falls through to the HERMES_HOME default
     (database_path == "") instead of the inherited absolute path,
  3. no fd in the live process points at the inherited DB path — this
     covers opens that happened during collection/bootstrap, because an
     fd opened at import time is still open here.

RED (before the guard): assertions 1-3 fail with an inherited env.
GREEN (with the guard): all pass, and no file appears at the sentinel path.
"""
import os
import sys
from pathlib import Path

import pytest

# The hermes_lcm package is aliased by tests/conftest.py (the repo root is
# the plugin directory, not a package named hermes_lcm); collection runs
# conftest first, so this import resolves at runtime.
from hermes_lcm.config import LCMConfig  # noqa: E402

LINUX = sys.platform.startswith("linux")


def _live_fd_targets():
    targets = []
    for fd in Path("/proc/self/fd").iterdir():
        try:
            targets.append(os.readlink(str(fd)))
        except OSError:
            continue
    return targets


def test_no_lcm_env_survives_conftest_scrub():
    leftovers = sorted(k for k in os.environ if k.startswith("LCM_"))
    assert leftovers == [], f"LCM_* env leaked past the conftest guard: {leftovers}"


def test_from_env_does_not_bind_inherited_database_path():
    expected = os.environ.get("I9JW_EXPECT_DB", "")
    cfg = LCMConfig.from_env()
    if expected:
        assert cfg.database_path != expected, (
            "LCMConfig.from_env() re-bound the inherited database path "
            f"{expected!r}; the conftest scrub did not run early enough"
        )
    assert cfg.database_path == "", (
        "database_path must fall through to the HERMES_HOME default in a "
        f"scrubbed test process, got {cfg.database_path!r}"
    )


@pytest.mark.skipif(not LINUX, reason="/proc fd scan is Linux-only")
def test_zero_fds_on_inherited_database_path():
    expected = os.environ.get("I9JW_EXPECT_DB", "")
    if not expected:
        pytest.skip("no I9JW_EXPECT_DB sentinel; inherited-path fd check needs one")
    existed = os.environ.get("I9JW_EXPECT_DB_EXISTED")
    if existed is None:
        pytest.fail(
            "isolation guard did not run: conftest captured no pre-bootstrap "
            "baseline for the inherited DB path"
        )
    real = os.path.realpath(expected)

    def _is_inherited_db(target: str) -> bool:
        return target == real or target.startswith(
            (real + "-", real + ".")
        )  # covers -wal/-shm and sidecar lock files

    hits = [t for t in _live_fd_targets() if _is_inherited_db(t)]
    assert hits == [], f"test process holds live fds on the inherited DB {expected!r}"
    if existed == "0":
        assert not Path(expected).exists(), (
            f"inherited DB path {expected!r} did not exist before bootstrap "
            "but was materialized during the run"
        )
