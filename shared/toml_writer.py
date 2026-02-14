"""Minimal TOML writer for flat + one-level-nested dicts (no dependency needed)."""

from __future__ import annotations


def _format_value(value: object) -> str:
    """Format a Python value as a TOML value string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        # Escape backslashes and quotes
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    raise TypeError(f"Unsupported TOML value type: {type(value)}")


def dumps(data: dict) -> str:
    """Serialize a dict to TOML string.

    Supports flat keys and one level of nested tables (dict values).
    """
    lines: list[str] = []

    # First, write all top-level scalar keys
    for key, value in data.items():
        if not isinstance(value, dict):
            lines.append(f"{key} = {_format_value(value)}")

    if lines:
        lines.append("")

    # Then write each nested table
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"[{key}]")
            for k, v in value.items():
                if isinstance(v, dict):
                    continue  # Skip deeply nested (not supported)
                lines.append(f"{k} = {_format_value(v)}")
            lines.append("")

    return "\n".join(lines)
