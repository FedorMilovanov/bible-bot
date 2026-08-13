import asyncio
from types import SimpleNamespace

from utils import create_result_gif, generate_result_image


class NoAvatarBot:
    async def get_user_profile_photos(self, user_id, limit=1):
        return SimpleNamespace(total_count=0, photos=[])


def test_generate_result_png_smoke():
    payload = asyncio.run(
        generate_result_image(
            NoAvatarBot(),
            user_id=123,
            first_name="Тест",
            score=8,
            total=10,
            rank_name="📖 Богослов",
        )
    )
    assert payload is not None
    assert payload.startswith(b"\x89PNG\r\n\x1a\n")
    assert len(payload) > 1000


def test_generate_result_gif_smoke():
    buffer = asyncio.run(
        create_result_gif(
            score=8,
            total=10,
            rank_name="📖 Богослов",
            time_seconds=42.0,
            first_name="Тест",
        )
    )
    assert buffer is not None
    payload = buffer.getvalue()
    assert payload.startswith((b"GIF87a", b"GIF89a"))
    assert len(payload) > 1000
