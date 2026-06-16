from __future__ import annotations

import re


def safe_identifier(value: object) -> str:
    safe = re.sub(r"[^0-9A-Za-z_]+", "_", str(value)).strip("_")
    if not safe:
        return "unnamed"
    if safe[0].isdigit():
        return f"v_{safe}"
    return safe
