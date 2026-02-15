"""Piccolo ORM configuration."""

from pathlib import Path

from piccolo.conf.apps import AppRegistry
from piccolo.engine.sqlite import SQLiteEngine

DB_PATH = Path(__file__).resolve().parent / "data" / "coldpipe.db"

DB = SQLiteEngine(path=str(DB_PATH))

APP_REGISTRY = AppRegistry(apps=["piccolo_app"])
