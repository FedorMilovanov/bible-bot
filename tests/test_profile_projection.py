from web_api.routes import _public_user_document


def test_public_user_document_hides_internal_scoring_receipts_and_unknown_fields():
    document = {
        "_id": "123",
        "first_name": "Test",
        "total_points": 42,
        "easy_p1_attempts": 3,
        "easy_p1_best_score": 9,
        "miniapp_result_receipts": {
            "session-secret-state": {
                "points": 42,
                "daily_bonus": 5,
                "kind": "regular",
            }
        },
        "future_internal_state": {"must": "not leak by default"},
        "_hard_correct": 7,
    }

    public = _public_user_document(document)

    assert public == {
        "first_name": "Test",
        "total_points": 42,
        "easy_p1_attempts": 3,
        "easy_p1_best_score": 9,
    }
    assert "miniapp_result_receipts" not in public
    assert "future_internal_state" not in public
    assert all(not key.startswith("_") for key in public)
