import inspect
import re
import warnings

import pytest

import utils


def test_result_image_date_helper_is_python314_safe():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        value = utils._today_utc_display()

    assert re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", value)


def test_utils_module_no_longer_calls_deprecated_utcnow():
    assert ".utcnow(" not in inspect.getsource(utils)


@pytest.mark.asyncio
async def test_result_png_generation_is_deprecation_clean_under_python314():
    class EmptyPhotos:
        total_count = 0

    class Bot:
        async def get_user_profile_photos(self, user_id, limit=1):
            return EmptyPhotos()

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        png = await utils.generate_result_image(
            Bot(),
            user_id=1,
            first_name="Тест",
            score=8,
            total=10,
            rank_name="📖 Богослов",
        )

    assert png is not None
    assert png.startswith(b"\x89PNG\r\n\x1a\n")
