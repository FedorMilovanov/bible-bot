from datetime import datetime, timedelta

import legacy_battle_progress as progress
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


QUESTION = {"question": "Кто?", "options": ["Пётр", "Павел"], "correct": 0}


class CapturingCollection:
    def __init__(self, row):
        self.row = row
        self.query = None

    def find_one(self, query):
        self.query = query
        return self.row


def completed_battle(*, status="finalized", final_claimed=True):
    started = datetime(2026, 8, 11, 12, 0, 0)
    answered = started + timedelta(seconds=2)
    latency = 2.0
    points = 10 + round((7.0 - latency) / 7.0 * 7)
    return {
        "_id": "battle-1",
        "creator_id": 101,
        "opponent_id": 202,
        "status": status,
        "final_claimed": final_claimed,
        "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
        "questions": [QUESTION],
        "live_progress": {
            "creator": {
                "current_index": 1,
                "correct_count": 1,
                "points": points,
                "answers": [{
                    "index": 0,
                    "qid": progress.battle_question_id(QUESTION),
                    "user_answer": "Пётр",
                    "is_correct": True,
                    "points": points,
                    "latency_seconds": latency,
                    "answered_at": answered,
                }],
                "started_at": started,
                "question_sent_at": None,
            }
        },
    }


def test_completed_inputs_remain_recoverable_after_shared_finalization(monkeypatch):
    collection = CapturingCollection(completed_battle())
    monkeypatch.setattr(progress, "_battle_collection", lambda: collection)

    result = progress.completed_battle_result_inputs("battle-1", 101, "creator")

    assert result["score"] == 1
    assert result["total"] == 1
    assert result["time_seconds"] == 2.0
    assert result["points"] == 15
    assert collection.query["creator_id"] == 101
    assert collection.query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert "finalized" in collection.query["status"]["$in"]


def test_mutation_filter_still_excludes_finalized_state():
    query = progress._owned_battle_filter("battle-1", 101, "creator")

    assert query["status"] == {"$in": ["waiting", "in_progress"]}
    assert query["final_claimed"] == {"$ne": True}
