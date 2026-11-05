"""The deterministic test-only export signing pair: the seed the in-process export worker signs
with (tests/support/exports.py, tests/support/worker_tasks.py) and the public key the test
control plane publishes (services/control-plane/tests/conftest.py)."""

import base64

from sijil_core.export_report import public_key

TEST_SEED = bytes(range(32, 64))
TEST_SEED_B64 = base64.b64encode(TEST_SEED).decode()
TEST_PUBLIC_KEY = public_key(TEST_SEED)
TEST_PUBLIC_KEY_B64 = base64.b64encode(TEST_PUBLIC_KEY).decode()
