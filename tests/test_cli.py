"""CLI smoke tests using typer.testing.CliRunner."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from cli import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _tmp_db(tmp_path, monkeypatch):
    """Point the DB to a temporary directory for CLI tests."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("db.DB_PATH", db_path)
    monkeypatch.setattr("config.settings.DB_PATH", db_path)
    # Also set env var in case settings.load_settings() reads it
    monkeypatch.setenv("DB_PATH", str(db_path))


def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "coldpipe" in result.output.lower() or "Customer" in result.output


def test_db_init():
    result = runner.invoke(app, ["db", "init"])
    assert result.exit_code == 0
    assert "initialized" in result.output.lower() or "version" in result.output.lower()


def test_db_help():
    result = runner.invoke(app, ["db", "--help"])
    assert result.exit_code == 0


def test_leads_help():
    result = runner.invoke(app, ["leads", "--help"])
    assert result.exit_code == 0


def test_scrape_help():
    result = runner.invoke(app, ["scrape", "--help"])
    assert result.exit_code == 0


def test_campaign_help():
    result = runner.invoke(app, ["campaign", "--help"])
    assert result.exit_code == 0


def test_daemon_help():
    result = runner.invoke(app, ["daemon", "--help"])
    assert result.exit_code == 0


def test_leads_list_empty():
    """Fresh DB should have zero leads."""
    # First init the DB
    runner.invoke(app, ["db", "init"])
    result = runner.invoke(app, ["leads", "list"])
    assert result.exit_code == 0
    assert "0" in result.output
