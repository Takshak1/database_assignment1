from __future__ import annotations

from pathlib import Path
from typing import Dict

import pytest

from crud_query_engine import CRUDQueryEngine
from schema_registry import SchemaRegistry


@pytest.fixture()
def registry(tmp_path: Path) -> SchemaRegistry:
    """Ephemeral registry backed by a temp SQLite DB."""

    return SchemaRegistry(db_path=str(tmp_path / "report_registry.db"))


@pytest.fixture()
def sample_schema_definition() -> Dict[str, Dict[str, object]]:
    """Canonical schema used across requirement tests."""

    return {
        "username": {"type": "string", "unique": True},
        "post_id": {"type": "integer"},
        "comments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "time": {"type": "integer"},
                },
            },
        },
        "profile": {
            "type": "object",
            "properties": {
                "address": {"type": "string"},
                "city": {"type": "string"},
            },
        },
    }


@pytest.fixture()
def stored_schema(registry: SchemaRegistry, sample_schema_definition: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    """Registers the canonical schema once and returns full metadata bundle."""

    return registry.register_schema("post", sample_schema_definition)


@pytest.fixture()
def crud_engine(registry: SchemaRegistry) -> CRUDQueryEngine:
    """Helper fixture for CRUD planning/validation tests."""

    return CRUDQueryEngine(registry=registry)
