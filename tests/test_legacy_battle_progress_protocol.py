from datetime import datetime
from types import SimpleNamespace

import pytest

import legacy_battle_progress as progress
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


class CapturingCollection:
    def __init__(self):
        self.query = None

    def find_one(self, query):
        self.query = query
        return None


def test_progress_owner_lookup_requires_durable_protocol(monkeypatch):
    collection = CapturingCollection()
    monkeypatch.setattr(progress, "_battle_collection", lambda: collection)
    monkeypatch.setattr(
        progress,
        "_database",
        lambda: SimpleNamespace(_now_utc=lambda: datetime(2026, 8, 11, 12, 0, 0)),
    )

    with pytest.raises(progress.LegacyBattleProgressConflict):
        progress.ensure_battle_progress("battle-1", 101, "creator")

    assert collection.query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert collection.query["creator_id"] == 101
