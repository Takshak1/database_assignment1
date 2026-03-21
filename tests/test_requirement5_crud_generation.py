"""Requirement 5: CRUD query generation across SQL + Mongo plans."""

from __future__ import annotations

from typing import Dict


def test_read_plan_contains_sql_statement(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "comments"],
            "filters": {"username": "neo"},
            "limit": 25,
        },
    )

    assert plan["sql"]["statement"].startswith("SELECT")
    assert plan["merge"]["merge_key"], "Merge plan should expose a key for stitching"
    comments_location = next(loc for loc in plan["field_locations"] if loc["requested"] == "comments")
    assert comments_location["storage"] in {"sql", "mongo"}


def test_filters_translate_into_sql_parameters(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username"],
            "filters": {"username": "trinity"},
        },
    )

    where_clause = plan["sql"]["where"]
    params = plan["sql"]["parameters"]
    assert "username" in where_clause
    assert params
    assert list(params.values())[0] == "trinity"


def test_merge_plan_describes_response_shape(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "comments"],
        },
    )

    merge_plan = plan["merge"]
    assert merge_plan["strategy"]
    assert "response_shape" in merge_plan
    assert merge_plan["response_shape"]["requested_fields"] == ["username", "comments"]
