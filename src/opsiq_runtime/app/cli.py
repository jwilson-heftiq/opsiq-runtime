from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from opsiq_runtime.application.registry import Registry
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.application.runner import Runner
from opsiq_runtime.app.factory import create_adapters
from opsiq_runtime.observability.logging import configure_logging


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="OpsIQ Runtime CLI")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run a primitive once")
    run_parser.add_argument("--tenant", required=True, dest="tenant_id")
    run_parser.add_argument("--primitive", required=True, dest="primitive_name")
    run_parser.add_argument("--config", required=True, dest="config_version")
    run_parser.add_argument("--primitive-version", default="1.0.0")
    run_parser.add_argument("--as-of", dest="as_of_ts")
    run_parser.add_argument("--correlation-id", dest="correlation_id")

    args = parser.parse_args()
    if args.command != "run":
        parser.print_help()
        return

    ctx = RunContext.from_args(
        tenant_id=args.tenant_id,
        primitive_name=args.primitive_name,
        primitive_version=args.primitive_version,
        as_of_ts=parse_datetime(args.as_of_ts) or datetime.now(timezone.utc),
        config_version=args.config_version,
        correlation_id=args.correlation_id,
    )

    config_provider, inputs_repo, outputs_repo, event_publisher, lock_manager = create_adapters(
        correlation_id=args.correlation_id
    )
    runner = Runner(
        config_provider=config_provider,
        inputs_repo=inputs_repo,
        outputs_repo=outputs_repo,
        event_publisher=event_publisher,
        lock_manager=lock_manager,
        registry=Registry(),
    )
    summary = runner.run(ctx)
    print(summary)


if __name__ == "__main__":
    main()

