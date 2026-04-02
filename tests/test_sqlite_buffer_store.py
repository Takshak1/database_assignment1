from pathlib import Path

from buffer_storage import SQLiteBufferStore


def test_buffer_store_persists_field(tmp_path: Path) -> None:
    db_path = tmp_path / "buffer.db"
    store = SQLiteBufferStore(db_path=str(db_path))

    record = {"username": "alice", "score": 42}
    buffer_id = store.store_field("score", 42, record)

    assert buffer_id == 1
    assert store.count() == 1

    entries = store.list_entries(limit=5)
    assert len(entries) == 1
    row = entries[0]
    assert row["field_name"] == "score"
    assert row["value"] == 42
    assert row["payload"]["username"] == "alice"


def test_buffer_store_handles_complex_values(tmp_path: Path) -> None:
    db_path = tmp_path / "buffer_complex.db"
    store = SQLiteBufferStore(db_path=str(db_path))

    value = {"nested": [1, 2, 3], "set": {"a", "b"}}
    payload = {"username": "bob", "extras": value}

    buffer_id = store.store_field("extras", value, payload)
    assert buffer_id == 1

    entries = store.list_entries()
    assert entries[0]["payload"]["extras"]["nested"] == [1, 2, 3]
    assert sorted(entries[0]["payload"]["extras"]["set"]) == ["a", "b"]

    store.clear()
    assert store.count() == 0
