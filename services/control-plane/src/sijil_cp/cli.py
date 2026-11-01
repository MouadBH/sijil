"""`sijil-cp` console entry: `serve` runs the API, `bootstrap-storage` prepares the object store."""

import argparse
import asyncio
import sys
import time
from collections.abc import Sequence

from botocore.exceptions import BotoCoreError, ClientError

from sijil_core.storage import S3ObjectStore, StorageSettings

_BOOTSTRAP_ATTEMPTS = 30


def serve() -> None:
    from sijil_cp.main import run

    run()


def bootstrap_storage() -> None:
    """Create the object-locked bucket if missing. Safe to run repeatedly.

    The store may still be starting when the one-shot `migrate` service reaches this point, so
    connection errors are retried for about a minute.
    """
    settings = StorageSettings()
    store = S3ObjectStore(settings)
    for attempt in range(1, _BOOTSTRAP_ATTEMPTS + 1):
        try:
            asyncio.run(store.ensure_bucket())
        except (BotoCoreError, ClientError) as exc:
            if attempt == _BOOTSTRAP_ATTEMPTS:
                raise
            print(  # noqa: T201
                f"object store not ready ({type(exc).__name__}), retrying", file=sys.stderr
            )
            time.sleep(2)
        else:
            print(f"bucket {settings.objectstore_bucket} ready")  # noqa: T201
            return


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="sijil-cp")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help="run the API server")
    sub.add_parser("bootstrap-storage", help="create the object-locked original bucket")
    args = parser.parse_args(argv)
    if args.command == "serve":
        serve()
    else:
        bootstrap_storage()


if __name__ == "__main__":
    main()
