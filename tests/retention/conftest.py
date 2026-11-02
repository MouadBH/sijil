"""The lock suite drives the control-plane API like its own tests do: the control plane's test
fixtures (app on the test database, signed-in actors, the recording enqueuer, the clock) are
loaded from its conftest, which is not an importable package (`control-plane` has a hyphen).
"""

import importlib.util
from pathlib import Path

_PATH = Path(__file__).parents[2] / "services" / "control-plane" / "tests" / "conftest.py"
_SPEC = importlib.util.spec_from_file_location("sijil_retention_cp_conftest", _PATH)
assert _SPEC is not None and _SPEC.loader is not None  # noqa: S101 - import-time invariant
_CP = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_CP)

make_db_app = _CP.make_db_app
csrf_client_for = _CP.csrf_client_for
enqueuer = _CP.enqueuer
clock = _CP.clock
cp_app = _CP.cp_app
make_actor = _CP.make_actor
