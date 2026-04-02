"""SQLite persistence for buffered ingestion fields.

This module complements :mod:`storage_manager` by offering a lightweight,
persistent landing zone for any fields that are still classified as
"buffer". Each buffered field is stored together with the full payload so
that it can be replayed once enough schema intelligence is available.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional


class SQLiteBufferStore:
    """Persists buffer-designated fields and their payload context in SQLite."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path or os.getenv("BUFFER_DB_PATH", "buffer_store.db")
        self._ensure_table()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def store_field(
        self,
        field_name: str,
        value: Any,
        payload: Dict[str, Any],
        *,
        reason: str = "buffered_pending_schema",
    ) -> int:
        """Persist a field + payload snapshot, returning the inserted row id."""

        with self._get_conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO buffered_fields (
                    field_name,
                    value_json,
                    payload_json,
                    reason,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    field_name,
                    json.dumps(value, default=self._fallback_serializer),
                    json.dumps(payload, default=self._fallback_serializer),
                    reason,
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def list_entries(self, *, limit: int = 50) -> List[Dict[str, Any]]:
        """Return the most recent buffered entries (value/payload decoded)."""

        query = """
            SELECT id, field_name, value_json, payload_json, reason, created_at
            FROM buffered_fields
            ORDER BY created_at DESC
            LIMIT ?
        """
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, (limit,)).fetchall()

        results: List[Dict[str, Any]] = []
        for row in rows:
            results.append(
                {
                    "id": row["id"],
                    "field_name": row["field_name"],
                    "value": self._safe_json_load(row["value_json"]),
                    "payload": self._safe_json_load(row["payload_json"]),
                    "reason": row["reason"],
                    "created_at": row["created_at"],
                }
            )
        return results

    def count(self) -> int:
        with self._get_conn() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM buffered_fields")
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def clear(self) -> None:
        with self._get_conn() as conn:
            conn.execute("DELETE FROM buffered_fields")
            conn.commit()

    def close(self) -> None:
        """No-op hook for API symmetry."""
        return None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_table(self) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS buffered_fields (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    field_name TEXT NOT NULL,
                    value_json TEXT,
                    payload_json TEXT,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _safe_json_load(self, raw: Optional[str]) -> Any:
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    def _fallback_serializer(self, value: Any) -> Any:
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()  # type: ignore[no-any-return]
            except Exception:
                return str(value)
        if isinstance(value, (set, bytes)):
            if isinstance(value, set):
                return list(value)
            return value.decode("utf-8", "ignore")
        return value


__all__ = ["SQLiteBufferStore"]
