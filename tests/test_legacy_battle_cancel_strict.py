from copy import deepcopy
from types import SimpleNamespace

import pytest
from pymongo.errors import PyMongoError

import legacy_battle_cancel as cancel
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


class Collection:
    def __init__(self, deleted_count=1):
        self.deleted_count = deleted_count
        self.query = None
        self.error = None

    def delete_one(self, query):
        self.query = deepcopy(query)
        if self.error:
            raise self.error
        return SimpleNamespace(deleted_count=self.deleted_count)


def test_cancel_filter_requires_no_live_progress(monkeypatch):
    collection = Collection()
    monkeypatch.setattr(cancel, "_collection", lambda: collection)

    assert cancel.cancel_unstarted_battle("b1", 42) is True
    assert collection.query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert collection.query["live_progress"] == {"$exists": False}
    assert collection.query["creator_finished"] == {"$ne": True}
    assert collection.query["opponent_finished"] == {"$ne": True}
    assert collection.query["final_claimed"] == {"$ne": True}


def test_started_or_changed_battle_is_not_reported_cancelled(monkeypatch):
    collection = Collection(deleted_count=0)
    monkeypatch.setattr(cancel, "_collection", lambda: collection)
    assert cancel.cancel_unstarted_battle("b1", 42) is False


def test_cancel_storage_outage_is_explicit(monkeypatch):
    collection = Collection()
    collection.error = PyMongoError("down")
    monkeypatch.setattr(cancel, "_collection", lambda: collection)
    with pytest.raises(cancel.LegacyBattleCancelUnavailable, match="failed"):
        cancel.cancel_unstarted_battle("b1", 42)
