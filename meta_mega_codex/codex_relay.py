#!/usr/bin/env python3
"""Relay orchestrator for the Meta Mega Codex runtime."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from codex_executor import CodexExecutor

BASE_DIR = Path(__file__).parent
PIPELINE = ["relay", "executor", "logger", "evaluation", "digest"]


def _deep_merge(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for key, value in incoming.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data


def _load_stack_with_includes(path: Path, ancestry: List[Path] | None = None) -> Dict[str, Any]:
    ancestry = ancestry or []
    if path in ancestry:
        raise ValueError(f"Circular include detected: {' -> '.join(str(p) for p in ancestry + [path])}")
    data = _load_yaml(path)
    includes = data.get("include") or []
    merged: Dict[str, Any] = {}
    for include_name in includes:
        include_path = path.parent / include_name
        merged = _deep_merge(merged, _load_stack_with_includes(include_path.resolve(), ancestry + [path]))
    body = {k: v for k, v in data.items() if k != "include"}
    return _deep_merge(merged, body)


def _discover_stack(
    persona: str, role: str, stacks_dir: Path, explicit_stack: Path | None
) -> Tuple[Dict[str, Any], Path]:
    if explicit_stack:
        if explicit_stack.is_absolute():
            target = explicit_stack
        else:
            candidate = (BASE_DIR / explicit_stack)
            target = candidate if candidate.exists() else (stacks_dir / explicit_stack)
        if not target.exists():
            raise FileNotFoundError(f"Stack file {target} does not exist.")
        return _load_stack_with_includes(target.resolve()), target.resolve()

    for candidate in sorted(stacks_dir.glob("*.yaml")):
        stack = _load_stack_with_includes(candidate.resolve())
        routing = stack.get("routing") or {}
        if routing.get("persona") == persona and routing.get("role") == role:
            return stack, candidate.resolve()
    raise LookupError(f"No stack found for persona={persona} role={role}.")


def _load_policies() -> Dict[str, Any]:
    policy_path = BASE_DIR / "config" / "policies.yaml"
    if not policy_path.exists():
        return {}
    with policy_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _normalize_run_dir(path_arg: str) -> Path:
    path = Path(path_arg)
    if not path.is_absolute():
        path = BASE_DIR / path
    path.mkdir(parents=True, exist_ok=True)
    (path / "outputs").mkdir(parents=True, exist_ok=True)
    return path


def _parse_payload(payload_str: str, max_bytes: int | None) -> Dict[str, Any]:
    encoded = payload_str.encode("utf-8")
    if max_bytes and len(encoded) > max_bytes:
        raise ValueError(f"Payload exceeds relay policy limit of {max_bytes} bytes.")
    parsed = json.loads(payload_str or "{}")
    if not isinstance(parsed, dict):
        raise TypeError("Payload must be a JSON object.")
    return parsed


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Relay persona requests to the executor.")
    parser.add_argument("--persona", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--payload", default="{}")
    parser.add_argument("--stacks-dir", default="stacks")
    parser.add_argument("--stack-file", help="Override stack file path.")
    parser.add_argument("--run-dir", required=True, help="Run directory under the PreservationVault.")
    return parser


def main() -> None:
    parser = _build_cli()
    args = parser.parse_args()

    stacks_dir = (BASE_DIR / args.stacks_dir).resolve()
    if not stacks_dir.exists():
        raise FileNotFoundError(f"Stacks directory {stacks_dir} does not exist.")

    explicit_stack = Path(args.stack_file) if args.stack_file else None
    stack_config, stack_path = _discover_stack(args.persona, args.role, stacks_dir, explicit_stack)

    policies = _load_policies()
    payload = _parse_payload(args.payload, policies.get("relay", {}).get("max_payload_bytes"))

    executor = CodexExecutor(stack_config=stack_config, policies=policies.get("executor", {}))
    executor_result = executor.run(payload)

    run_dir = _normalize_run_dir(args.run_dir)
    agent_name = executor_result["meta"].get("agent_name") or "agent"
    output_path = run_dir / "outputs" / f"{agent_name}.json"
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(executor_result, handle, indent=2)
        handle.write("\n")

    try:
        output_reference = str(output_path.relative_to(BASE_DIR))
    except ValueError:
        output_reference = str(output_path)

    relay_report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stack_file": str(stack_path),
        "persona": args.persona,
        "role": args.role,
        "pipeline": PIPELINE,
        "cfms_invariants": stack_config.get("cfms_invariants", {}),
        "meta": stack_config.get("meta", {}),
        "payload": payload,
        "outputs": {
            "executor": output_reference,
        },
        "policies_applied": {
            "relay": policies.get("relay"),
            "executor": policies.get("executor"),
        },
    }

    json.dump(relay_report, fp=sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
