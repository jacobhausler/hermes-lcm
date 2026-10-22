#!/usr/bin/env bash
# estate-ci py component hook for hermes-lcm (NEXTGEN-CI.md (d): the leg step runs
# tools/ci/component/<component>.sh; ledger grammar is emitted by the leg wrapper).
#
# Hermetic law (D16/W): fresh venv per lane, no cross-lane cache, uv installs
# hash-verified (interpreter downloads carry sha256; wheels via --require-hashes
# on the committed uv-compiled lock). floor/tip = python arm (spec (b) lcm row);
# hermes tip = hermes-agent pin inside the lock (bump = visible PR).
set -euo pipefail

PYV=$(/usr/bin/python3 -c 'import json,os; print(json.loads(os.environ["LEG_JSON"])["python"])')
LOCK="tools/ci/requirements-ci-${PYV}.lock"
if [ ! -f "$LOCK" ]; then
  echo "py-hook: no lock for python arm $PYV" >&2
  exit 2
fi

export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"
if ! command -v uv >/dev/null 2>&1; then
  # Runner image carries no uv: bootstrap it hermetically — own venv, single
  # sha256-pinned wheel, --require-hashes. From here on every download
  # (managed interpreters included) is hash-verified.
  UVV=$(mktemp -d)/uvvenv
  /usr/bin/python3 -m venv "$UVV"
  "$UVV/bin/pip" install -q --no-deps --require-hashes -r tools/ci/requirements-uv.lock
  UV="$UVV/bin/uv"
else
  UV=$(command -v uv)
fi

VENV=$(mktemp -d)/estate-venv
"$UV" python install -q "$PYV"
"$UV" venv -q --python "$PYV" "$VENV"
VIRTUAL_ENV="$VENV" "$UV" pip install -q --python "$VENV/bin/python" --require-hashes -r "$LOCK"
echo "py-hook: HASH-VERIFY ENABLED (interpreter sha256 + wheels --require-hashes)"
"$VENV/bin/python" scripts/validate_dependency_contract.py --report-environment

T0=$(date +%s)
OUT=$(mktemp)
set +e
VIRTUAL_ENV="$VENV" "$VENV/bin/python" -m pytest tests -q -p no:cacheprovider \
  --timeout="${ESTATE_CASE_TIMEOUT_S:-600}" --timeout-method=thread \
  -o cache_dir=/tmp/pytest-cache 2>&1 | tee "$OUT"
RC=${PIPESTATUS[0]}
set -e
WALL=$(( $(date +%s) - T0 ))
CASES=$(grep -oE '[0-9]+ passed' "$OUT" | tail -1 | grep -oE '^[0-9]+' || true)
CASES=${CASES:-0}
echo "CASES=$CASES WALL=$WALL" > .estate-leg-report
echo "py-hook: arm=$ESTATE_LEG_ID lane=$ESTATE_LANE/$ESTATE_LANES rc=$RC cases=$CASES wall=${WALL}s"
exit $RC
