"""Integration tests for insert/update/delete planning workflow."""

from __future__ import annotations

from crud_executor import HybridCRUDExecutor


def _payload() -> dict:
    return {
        "username": "user1",
        "post_id": 101,
        "comments": [
            {"text": "hello", "time": 1},
            {"text": "world", "time": 2},
        ],
        "profile": {"address": "Main St", "city": "NYC"},
    }


def test_insert_plan_splits_payload_using_mappings(crud_engine, stored_schema) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "insert",
            "payload": _payload(),
        },
    )

    assert plan["operation"] == "insert"
    assert plan["sql"]["order"][0] == "post"
    assert len(plan["sql"]["rows"]["comments"]) == 2
    assert plan["sql"]["rows"]["profile"][0]["address"] == "Main St"
    assert "foreign_keys" in plan["sql"]


def test_delete_entity_plan_cascades_reverse_order(crud_engine, stored_schema) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "delete",
            "strategy": "entity",
            "filters": {"post_id": 101},
        },
    )

    assert plan["operation"] == "delete"
    assert plan["strategy"] == "entity"
    assert plan["consistency"]["cascade"] is True
    assert plan["sql"]["tables"][-1] == "post"


def test_delete_subentity_targets_specific_table(crud_engine, stored_schema) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "delete",
            "strategy": "sub-entity",
            "filters": {
                "target": "comments",
                "criteria": {"post_id": 101, "comment_id": 1},
            },
        },
    )

    assert plan["strategy"] == "sub-entity"
    assert plan["sql"]["tables"] == ["comments"]
    assert plan["filters"] == {"post_id": 101, "comment_id": 1}


def test_update_simple_returns_delete_then_insert_plan(crud_engine, stored_schema) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "update",
            "strategy": "simple",
            "filters": {"post_id": 101},
            "payload": _payload(),
        },
    )

    assert plan["operation"] == "update"
    assert plan["strategy"] == "simple"
    assert plan["consistency"]["mode"] == "delete_then_insert"
    assert plan["delete"]["operation"] == "delete"
    assert plan["insert"]["operation"] == "insert"


def test_executor_uses_query_engine_for_write_dry_runs(registry, stored_schema) -> None:
    executor = HybridCRUDExecutor(registry=registry)
    insert_result = executor.execute(
        stored_schema["schema_id"],
        operation="insert",
        payload=_payload(),
        execute=False,
    )
    delete_result = executor.execute(
        stored_schema["schema_id"],
        operation="delete",
        filters={"target": "comments", "criteria": {"post_id": 101}, "post_id": 101},
        strategy="sub-entity",
        execute=False,
    )

    insert_details = insert_result.details
    delete_details = delete_result.details
    assert insert_details["plan"]["operation"] == "insert"
    assert "consistency" in insert_details["plan"]
    assert delete_details["plan"]["strategy"] == "sub-entity"
    assert delete_details["sql"]["tables"] == ["comments"]
