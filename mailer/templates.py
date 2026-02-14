"""Jinja2 plain-text template rendering for email sequences."""

from __future__ import annotations

from jinja2 import FileSystemLoader
from jinja2.sandbox import SandboxedEnvironment

from config.settings import TEMPLATES_DIR

_env = SandboxedEnvironment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=False,
    keep_trailing_newline=True,
)


def render_template(template_name: str, context: dict) -> str:
    """Render a plain-text template with the given context dict."""
    tpl = _env.get_template(template_name)
    return tpl.render(**context)
