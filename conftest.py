"""Put the repo root on sys.path for every pytest invocation style.

Without this, `pytest -q tests/` - the command both the Makefile and
ci.yml actually run - cannot import `reliability`, `incidents`, or
`ingestion`: bare `pytest` does not add the working directory to
sys.path, only `python -m pytest` does. That difference kept the
automatic CI gate red on main for five consecutive pushes while the
same suite passed locally, because local runs used `python -m pytest`.

pytest imports a rootdir conftest.py before collecting anything and, in
the default "prepend" import mode, puts its directory on sys.path -
so simply existing here fixes every invocation style at once
(bare pytest, python -m pytest, make test, an IDE runner), rather than
fixing the one command that happened to be noticed.
"""
import sys
from pathlib import Path

REPO_ROOT = str(Path(__file__).resolve().parent)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
