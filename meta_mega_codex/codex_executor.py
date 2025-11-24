#!/usr/bin/env python3
"""Codex executor module responsible for persona-specific advisory output."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

import yaml

BASE_DIR = Path(__file__).parent


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _coerce_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    raise TypeError("Payload must decode to a JSON object.")


def _coerce_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


@dataclass
class Advisory:
    summary: str
    recommended_actions: List[str]
    confidence: str
    risk_level: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "summary": self.summary,
            "recommended_actions": self.recommended_actions,
            "confidence": self.confidence,
            "risk_level": self.risk_level,
        }


class CodexExecutor:
    """Simple persona executor that fabricates structured advisory output."""

    def __init__(
        self, stack_config: MutableMapping[str, Any], policies: Optional[Dict[str, Any]] = None
    ) -> None:
        if not stack_config:
            raise ValueError("Stack configuration cannot be empty.")
        self.stack_config = stack_config
        self.policies = policies or {}
        agents = stack_config.get("agents") or []
        if not agents:
            raise ValueError("Stack must declare at least one agent.")
        self.agent = agents[0]

    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized_payload = _coerce_mapping(payload)
        advisory = self._generate_advisory(normalized_payload).as_dict()
        return {
            "meta": {
                "agent_name": self.agent.get("name"),
                "model": self.agent.get("model"),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
            "routing": self.stack_config.get("routing", {}),
            "cfms_invariants": self.stack_config.get("cfms_invariants", {}),
            "payload": normalized_payload,
            "advice": advisory,
        }

    def _generate_advisory(self, payload: Dict[str, Any]) -> Advisory:
        summary_prefix = self.policies.get("summary_prefix", "Advisor Insight")
        objectives = _coerce_list(payload.get("objectives"))
        blockers = _coerce_list(payload.get("blockers"))
        context = payload.get("context") or "No additional context supplied."

        summary_parts = [summary_prefix, context]
        if objectives:
            summary_parts.append(f"Core objective: {objectives[0]}")
        summary = " — ".join(summary_parts)

        recommended_actions = list(self.policies.get("default_actions", []))
        if blockers:
            recommended_actions.append(f"Resolve blocker: {blockers[0]}")
        if objectives:
            recommended_actions.append(f"Advance objective: {objectives[0]}")
        if not recommended_actions:
            recommended_actions.append("Document a concrete next step.")

        risk_level = payload.get("risk_level") or ("low" if not blockers else "medium")
        confidence = "balanced" if blockers else "high"

        return Advisory(
            summary=summary,
            recommended_actions=recommended_actions,
            confidence=confidence,
            risk_level=risk_level,
        )


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a Codex executor directly.")
    parser.add_argument("--stack-file", required=True, help="Path to the stack YAML file.")
    parser.add_argument("--payload", default="{}", help="JSON payload.")
    return parser


def main() -> None:
    parser = _build_cli()
    args = parser.parse_args()

    stack_path = Path(args.stack_file)
    stack_config = _load_yaml(stack_path)
    policies = _load_yaml(BASE_DIR / "config" / "policies.yaml").get("executor", {})
    payload = json.loads(args.payload)

    executor = CodexExecutor(stack_config=stack_config, policies=policies)
    result = executor.run(payload)
    json.dump(result, fp=sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
