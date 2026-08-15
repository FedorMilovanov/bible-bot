from __future__ import annotations

import pytest

from course_catalog import CourseCatalogError
from web_api.quiz_start import _resolve_normal_course


@pytest.mark.parametrize(
    "field,value",
    [
        ("pool", "competitive_all"),
        ("ranked", True),
        ("multiplier", 99),
        ("score_multiplier", 99),
        ("scoring_mode", "scored"),
        ("points_per_question", 999),
    ],
)
def test_normal_course_rejects_client_policy_authority_fields(field, value):
    with pytest.raises(CourseCatalogError, match="client cannot override"):
        _resolve_normal_course(
            {"course_key": "chapter2", field: value},
            "relaxed",
        )


def test_legacy_pool_key_remains_catalog_bound_compatibility_only():
    entry = _resolve_normal_course({"pool_key": "chapter2"}, "relaxed")
    assert entry.key == "chapter2"
    assert entry.pool_key == "chapter2"

    with pytest.raises(CourseCatalogError, match="not an exposed Mini App course"):
        _resolve_normal_course({"pool_key": "competitive_all"}, "relaxed")
