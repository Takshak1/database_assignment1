"""Requirement 4: Metadata system captures structural + semantic information."""

from __future__ import annotations

from typing import Dict


def test_metadata_tracks_field_level_attributes(stored_schema: Dict[str, object]) -> None:
    fields = {field["field_name"]: field for field in stored_schema["fields"]}
    username = fields["username"]

    assert username["metadata"]["type"] == "string"
    assert username["is_unique"] is True
    assert username["parent_field"] is None


def test_analysis_entries_capture_pipeline_and_classification(stored_schema: Dict[str, object]) -> None:
    analysis_entries = {entry["field_path"]: entry for entry in stored_schema["analysis"]["entries"]}

    assert analysis_entries["comments"]["classification"] == "repeating_entity"
    assert analysis_entries["comments"]["pipeline"] == "sql"

    profile_entry = analysis_entries["profile"]
    assert profile_entry["pipeline"] in {"sql", "mongo", "buffer"}
    assert profile_entry["pipeline_reason"]


def test_analysis_summary_reports_totals(stored_schema: Dict[str, object]) -> None:
    summary = stored_schema["analysis"]["summary"]
    assert summary["pipelines"]["sql"] >= 1
    assert "pipelines" in summary
    assert summary.get("pipeline_reasons"), "Expected reason index to be populated for explainability"
