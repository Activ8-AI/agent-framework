#!/usr/bin/env python3
"""Generate aggregate digests from PreservationVault runs."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from codex_utils import load_yaml

BASE_DIR = Path(__file__).parent


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _collect_runs(vault_dir: Path, limit: int | None) -> List[Dict[str, Any]]:
    runs_root = vault_dir / "runs"
    if not runs_root.exists():
        return []
    run_dirs: List[Path] = []
    for candidate in runs_root.iterdir():
        if not candidate.is_dir():
            continue
        if (candidate / "relay.json").exists():
            run_dirs.append(candidate)
            continue
        for nested in candidate.iterdir():
            if nested.is_dir():
                run_dirs.append(nested)

    run_dirs = sorted(run_dirs, reverse=True)
    if limit is not None:
        run_dirs = run_dirs[:limit]

    collected: List[Dict[str, Any]] = []
    for run_path in run_dirs:
        relay = _load_json(run_path / "relay.json")
        evaluation = _load_json(run_path / "evaluation.json")
        logger = _load_json(run_path / "logger.json")
        collected.append(
            {
                "run_dir": str(run_path),
                "timestamp": relay.get("timestamp"),
                "persona": relay.get("persona"),
                "role": relay.get("role"),
                "pipeline": relay.get("pipeline"),
                "outputs": relay.get("outputs", {}),
                "evaluation_schema": evaluation.get("schema_version"),
                "evaluation_criteria": len(evaluation.get("criteria", [])),
                "logger_present": bool(logger),
            }
        )
    return collected


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce a digest report from PreservationVault runs.")
    parser.add_argument("--vault", default="PreservationVault", help="Path to the PreservationVault directory.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of runs included.")
    parser.add_argument("--output", help="Optional path to write the digest JSON. Defaults to stdout.")
    return parser


def main() -> None:
    parser = _build_cli()
    args = parser.parse_args()

    vault_dir = Path(args.vault)
    if not vault_dir.is_absolute():
        vault_dir = (BASE_DIR / vault_dir).resolve()

    environment_config = load_yaml(BASE_DIR / "config" / "environment.yaml")
    policies = load_yaml(BASE_DIR / "config" / "policies.yaml").get("digest", {})
    limit = args.limit if args.limit is not None else policies.get("max_runs")

    runs = _collect_runs(vault_dir, limit)
    digest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": environment_config,
        "run_count": len(runs),
        "runs": runs,
    }

    if policies.get("include_history"):
        digest["history"] = {"vault_path": str(vault_dir), "limit": limit}

    output = json.dumps(digest, indent=2)
    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = (BASE_DIR / out_path).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
