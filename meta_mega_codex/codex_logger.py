#!/usr/bin/env python3
"""Run logger for Meta Mega Codex executions."""

from __future__ import annotations

import argparse
import json
import platform
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from codex_utils import load_yaml, redact

BASE_DIR = Path(__file__).parent

def _collect_environment() -> Dict[str, Any]:
    env = {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "hostname": socket.gethostname(),
    }
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=BASE_DIR,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            env["git_sha"] = result.stdout.strip()
    except Exception:  # pragma: no cover
        pass
    return env


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Log relay outputs and environment context.")
    parser.add_argument("--run-dir", required=True, help="Run directory produced by the relay.")
    parser.add_argument("--record-env", action="store_true", help="Persist environment metadata.")
    return parser


def main() -> None:
    parser = _build_cli()
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.is_absolute():
        run_dir = (BASE_DIR / run_dir).resolve()

    relay_file = run_dir / "relay.json"
    if not relay_file.exists():
        raise FileNotFoundError(f"Relay output {relay_file} not found.")

    with relay_file.open("r", encoding="utf-8") as handle:
        relay_data = json.load(handle)

    policies = load_yaml(BASE_DIR / "config" / "policies.yaml").get("logger", {})
    redact_keys = policies.get("redact_keys", [])
    sanitized_payload = redact(relay_data.get("payload", {}), redact_keys)

    log_record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_dir": str(run_dir),
        "persona": relay_data.get("persona"),
        "role": relay_data.get("role"),
        "pipeline": relay_data.get("pipeline"),
        "payload": sanitized_payload,
        "outputs": relay_data.get("outputs"),
    }

    if args.record_env:
        log_record["environment"] = _collect_environment()

    log_path = run_dir / "logger.json"
    with log_path.open("w", encoding="utf-8") as handle:
        json.dump(log_record, handle, indent=2)
        handle.write("\n")

    json.dump({"logged": True, "logger_file": str(log_path)}, fp=sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
