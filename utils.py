"""
utils.py — вспомогательные функции бота.
Содержит: безопасная отправка, обрезка текста, генерация картинки результатов.
"""

import io
import re
import time
import asyncio
from datetime import datetime

from telegram import Update

# ─────────────────────────────────────────────
# БЕЗОПАСНАЯ ОТПРАВКА (исправление Markdown-обрезки)
# ─────────────────────────────────────────────
MAX_MSG_LEN = 3900

_MD_TAGS = re.compile(r'(\*\*|__|\*|_|`|```)')


def _close_open_tags(text: str) -> str:
    """
    Закрывает незакрытые Markdown-теги после обрезки.
    Работает с Telegram Markdown (не MarkdownV2).
    """
    stack = []
    pairs = {"*": "*", "_": "_", "`": "`"}
    i = 0
    while i < len(text):
        # triple backtick
        if text[i:i+3] == "```":
            if stack and stack[-1] == "```":
                stack.pop()
            else:
                stack.append("```")
            i += 3
            continue
        # single markers
        ch = text[i]
        if ch in pairs:
            if stack and stack[-1] == ch:
                stack.pop()
            else:
                stack.append(ch)
        i += 1

    # Закрываем в обратном порядке
    for tag in reversed(stack):
        text += tag
    return text


def safe_truncate(text: str, limit: int = MAX_MSG_LEN) -> str:
    """
    Обрезает текст до limit символов, закрывая открытые Markdown-теги.
    Обрезка происходит по последнему пробелу/переносу строки перед лимитом.
    """
    if len(text) <= limit:
        return text

    # Ищем удобное место для обрезки (не посреди слова)
    cut_pos = limit - 3
    for sep in ('\n', ' '):
        pos = text.rfind(sep, 0, cut_pos)
        if pos > cut_pos - 200:
            cut_pos = pos
            break

    truncated = text[:cut_pos] + "…"
    return _close_open_tags(truncated)


async def safe_send(target, text: str, **kwargs):
    """
    Безопасная отправка сообщения.
    Пробует Markdown, при ошибке — plain text.
    Обрезает текст, закрывая Markdown-теги.
    """
    text = safe_truncate(text)
    try:
        return await target.reply_text(text, parse_mode="Markdown", **kwargs)
    except Exception:
        try:
            kwargs.pop("parse_mode", None)
            return await target.reply_text(text, **kwargs)
        except Exception as e:
            print(f"safe_send failed: {e}")
            return None


async def safe_edit(query, text: str, **kwargs):
    """
    Безопасное редактирование сообщения через callback query.
    """
    text = safe_truncate(text)
    try:
        return await query.edit_message_text(text, parse_mode="Markdown", **kwargs)
    except Exception:
        try:
            kwargs.pop("parse_mode", None)
            return await query.edit_message_text(text, **kwargs)
        except Exception as e:
            print(f"safe_edit failed: {e}")
            return None


# ─────────────────────────────────────────────
# ГЕНЕРАЦИЯ КАРТИНКИ РЕЗУЛЬТАТОВ (задание 4.2)
# ─────────────────────────────────────────────

RANK_TABLE = [
    (95, "🌟 Апостол знания"),
    (80, "📖 Богослов"),
    (65, "🙏 Верный ученик"),
    (50, "📚 Искатель истины"),
    (0,  "🌱 Новичок"),
]


def get_rank_name(percentage: float) -> str:
    for threshold, name in RANK_TABLE:
        if percentage >= threshold:
            return name
    return "🌱 Новичок"


async def generate_result_image(
    bot,
    user_id: int,
    first_name: str,
    score: int,
    total: int,
    rank_name: str,
) -> bytes | None:
    """
    Генерирует PNG-картинку с результатами через Pillow.
    Возвращает bytes или None при ошибке.
    Аватарка загружается через Telegram Bot API; при отсутствии — заглушка.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageOps
    except ImportError:
        print("Pillow not installed — skipping image generation")
        return None

    pct = round(score / total * 100)

    # ── Скачиваем аватарку ──────────────────────────────────────
    avatar_img = None
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file_obj = await photos.photos[0][-1].get_file()
            file_bytes = await file_obj.download_as_bytearray()
            avatar_img = Image.open(io.BytesIO(bytes(file_bytes))).convert("RGBA")
            avatar_img = avatar_img.resize((120, 120))
            # Круглая маска
            mask = Image.new("L", (120, 120), 0)
            from PIL import ImageDraw as _ID
            _ID.Draw(mask).ellipse((0, 0, 120, 120), fill=255)
            avatar_img.putalpha(mask)
    except Exception as e:
        print(f"Avatar load failed: {e}")
        # Заглушка — круг с инициалом
        avatar_img = Image.new("RGBA", (120, 120), (70, 130, 180, 255))
        draw_tmp = ImageDraw.Draw(avatar_img)
        initial = (first_name[0].upper() if first_name else "?")
        try:
            font_big = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 60)
        except Exception:
            font_big = ImageFont.load_default()
        draw_tmp.text((30, 25), initial, fill=(255, 255, 255), font=font_big)

    # ── Создаём холст ───────────────────────────────────────────
    W, H = 600, 280
    img = Image.new("RGB", (W, H), (18, 18, 30))
    draw = ImageDraw.Draw(img)

    # Фоновый градиент (простой)
    for y in range(H):
        r = int(18 + y / H * 20)
        g = int(18 + y / H * 10)
        b = int(30 + y / H * 40)
        draw.line([(0, y), (W, y)], fill=(r, g, b))

    # Вставляем аватарку
    img.paste(avatar_img, (30, (H - 120) // 2), avatar_img.split()[3])

    # Шрифты
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        font_sub   = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
        font_score = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 52)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
    except Exception:
        font_title = font_sub = font_score = font_small = ImageFont.load_default()

    x_text = 180

    # Имя
    name_truncated = first_name[:20] if first_name else "Игрок"
    draw.text((x_text, 30), name_truncated, fill=(220, 220, 255), font=font_title)

    # Ранг
    draw.text((x_text, 68), rank_name, fill=(150, 200, 255), font=font_sub)

    # Счёт — крупно
    score_str = f"{score}/{total}"
    draw.text((x_text, 100), score_str, fill=(255, 215, 0), font=font_score)

    # Процент
    draw.text((x_text, 165), f"{pct}%  правильных ответов", fill=(180, 180, 210), font=font_sub)

    # Прогресс-бар
    bar_x, bar_y = x_text, 200
    bar_w, bar_h = W - x_text - 30, 18
    draw.rounded_rectangle([bar_x, bar_y, bar_x + bar_w, bar_y + bar_h],
                            radius=9, fill=(40, 40, 60))
    fill_w = int(bar_w * pct / 100)
    if fill_w > 0:
        color = (80, 200, 120) if pct >= 70 else (200, 160, 60) if pct >= 50 else (200, 80, 80)
        draw.rounded_rectangle([bar_x, bar_y, bar_x + fill_w, bar_y + bar_h],
                                radius=9, fill=color)

    # Нижняя подпись
    draw.text((x_text, 230), "Библейский тест-бот · 1 Петра", fill=(80, 80, 120), font=font_small)

    # Конвертируем в bytes
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ─────────────────────────────────────────────
# GARBAGE COLLECTION user_data (задание 2.1)
# ─────────────────────────────────────────────

async def cleanup_stale_userdata(context):
    """
    JobQueue task: удаляет из user_data записи с активностью >24ч.
    Запускается каждый час.
    """
    from bot import user_data  # импорт здесь, чтобы избежать кругового импорта
    now = time.time()
    stale = [
        uid for uid, data in list(user_data.items())
        if now - data.get("last_activity", now) > 86400  # 24 часа
    ]
    for uid in stale:
        user_data.pop(uid, None)
    if stale:
        print(f"🧹 GC: удалено {len(stale)} устаревших записей user_data")
