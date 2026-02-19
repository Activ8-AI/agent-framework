from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


def load_yaml(path: Path, *, required: bool = False) -> Dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"YAML file {path} does not exist.")
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise TypeError(f"YAML file {path} must contain a mapping at the top level.")
    return data


def redact(obj: Any, keys_to_redact: Any) -> Any:
    sensitive = {str(key) for key in (keys_to_redact or [])}

    if isinstance(obj, dict):
        redacted: Dict[str, Any] = {}
        for key, value in obj.items():
            if key in sensitive:
                redacted[key] = "***REDACTED***"
            else:
                redacted[key] = redact(value, keys_to_redact)
        return redacted

    if isinstance(obj, list):
        return [redact(item, keys_to_redact) for item in obj]

    return obj

