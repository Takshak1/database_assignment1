"""Requirement 7: Documentation sources enumerated in the report."""

from __future__ import annotations

from pathlib import Path


REPORT = Path("report.txt")


def test_report_contains_sources_section() -> None:
    text = REPORT.read_text(encoding="utf-8")
    assert "SOURCES OF INFORMATION" in text


def test_known_references_are_listed() -> None:
    text = REPORT.read_text(encoding="utf-8")
    for keyword in ("PostgreSQL", "MongoDB", "FastAPI", "Pydantic"):
        assert keyword in text


def test_all_numbered_sources_present() -> None:
    text = REPORT.read_text(encoding="utf-8")
    for idx in range(1, 7):
        assert f"{idx}. **" in text
