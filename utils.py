"""
utils.py — вспомогательные функции бота.
Безопасная отправка, обрезка текста, генерация картинки результатов, GC.
"""

import io
import os
import re
import time
import logging
import asyncio
from datetime import UTC, datetime

from telegram import Update
from telegram.error import BadRequest, RetryAfter, TimedOut

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
# КОНСТАНТЫ
# ═══════════════════════════════════════════════

MAX_MSG_LEN = 3900
MAX_CAPTION_LEN = 1000

# Таблица рангов
RANK_TABLE = [
    (95, "🌟 Апостол знания"),
    (80, "📖 Богослов"),
    (65, "🙏 Верный ученик"),
    (50, "📚 Искатель истины"),
    (30, "📝 Начинающий"),
    (0,  "🌱 Новичок"),
]

# Путь к шрифтам (Render / Docker обычно имеют DejaVu)
_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/TTF/DejaVuSans.ttf",
    "DejaVuSans-Bold.ttf",  # локальный fallback
    "DejaVuSans.ttf",
]

# Цвета для картинки
_COLORS = {
    "bg_top":       (18, 18, 30),
    "bg_bottom":    (38, 28, 70),
    "text_name":    (220, 220, 255),
    "text_rank":    (150, 200, 255),
    "text_score":   (255, 215, 0),
    "text_pct":     (180, 180, 210),
    "text_footer":  (80, 80, 120),
    "bar_bg":       (40, 40, 60),
    "bar_good":     (80, 200, 120),
    "bar_mid":      (200, 160, 60),
    "bar_bad":      (200, 80, 80),
    "avatar_bg":    (70, 130, 180, 255),
    "avatar_text":  (255, 255, 255),
}


def _today_utc_display() -> str:
    """UTC date for rendered result media, preserving the existing display format."""
    return datetime.now(UTC).strftime("%d.%m.%Y")


# ═══════════════════════════════════════════════
# MARKDOWN SAFETY
# ═══════════════════════════════════════════════

def _close_open_tags(text: str) -> str:
    """
    Закрывает незакрытые Markdown-теги после обрезки.
    Работает с Telegram Markdown (не MarkdownV2).
    """
    stack = []
    i = 0
    while i < len(text):
        # triple backtick
        if text[i:i + 3] == "```":
            if stack and stack[-1] == "```":
                stack.pop()
            else:
                stack.append("```")
            i += 3
            continue
        ch = text[i]
        if ch in ("*", "_", "`"):
            if stack and stack[-1] == ch:
                stack.pop()
            else:
                stack.append(ch)
        i += 1

    for tag in reversed(stack):
        text += tag
    return text


def safe_truncate(text: str, limit: int = MAX_MSG_LEN) -> str:
    """
    Обрезает текст до limit символов, закрывая открытые Markdown-теги.
    Обрезка — по последнему пробелу/переносу перед лимитом.
    """
    if not text:
        return ""
    if len(text) <= limit:
        return text

    cut_pos = limit - 3
    for sep in ("\n", ". ", " "):
        pos = text.rfind(sep, 0, cut_pos)
        if pos > cut_pos - 200:
            cut_pos = pos
            break

    truncated = text[:cut_pos] + "…"
    return _close_open_tags(truncated)


def escape_markdown(text: str) -> str:
    """Экранирует спецсимволы для Telegram Markdown."""
    chars = r"\_*[]()~`>#+-=|{}.!"
    for ch in chars:
        text = text.replace(ch, f"\\{ch}")
    return text


# ═══════════════════════════════════════════════
# БЕЗОПАСНАЯ ОТПРАВКА
# ═══════════════════════════════════════════════

async def safe_send(target, text: str, **kwargs):
    """
    Безопасная отправка сообщения.
    1. Пробует Markdown
    2. При ошибке — plain text
    3. При RetryAfter — ждёт и повторяет
    """
    text = safe_truncate(text)
    for attempt in range(3):
        try:
            return await target.reply_text(
                text, parse_mode="Markdown", **kwargs
            )
        except RetryAfter as e:
            logger.warning("RetryAfter in safe_send: %ss", e.retry_after)
            await asyncio.sleep(e.retry_after + 0.5)
        except BadRequest as e:
            if "can't parse" in str(e).lower():
                # Markdown битый — отправляем без форматирования
                kwargs.pop("parse_mode", None)
                try:
                    return await target.reply_text(text, **kwargs)
                except Exception as e2:
                    logger.error("safe_send plain fallback failed: %s", e2)
                    return None
            else:
                logger.error("safe_send BadRequest: %s", e)
                return None
        except TimedOut:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                logger.error("safe_send timed out after 3 attempts")
                return None
        except Exception as e:
            logger.error("safe_send failed: %s", e)
            return None
    return None


async def safe_edit(query, text: str, **kwargs):
    """
    Безопасное редактирование сообщения через callback query.
    """
    text = safe_truncate(text)
    for attempt in range(3):
        try:
            return await query.edit_message_text(
                text, parse_mode="Markdown", **kwargs
            )
        except RetryAfter as e:
            logger.warning("RetryAfter in safe_edit: %ss", e.retry_after)
            await asyncio.sleep(e.retry_after + 0.5)
        except BadRequest as e:
            err_str = str(e).lower()
            if "not modified" in err_str:
                return None  # контент не изменился — это нормально
            if "can't parse" in err_str:
                kwargs.pop("parse_mode", None)
                try:
                    return await query.edit_message_text(text, **kwargs)
                except Exception as e2:
                    logger.error("safe_edit plain fallback failed: %s", e2)
                    return None
            else:
                logger.error("safe_edit BadRequest: %s", e)
                return None
        except TimedOut:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                logger.error("safe_edit timed out after 3 attempts")
                return None
        except Exception as e:
            logger.error("safe_edit failed: %s", e)
            return None
    return None


async def safe_delete(bot, chat_id: int, message_id: int) -> bool:
    """Безопасное удаление сообщения. Возвращает True если удалено."""
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except BadRequest as e:
        if "not found" not in str(e).lower():
            logger.warning("safe_delete: %s", e)
        return False
    except Exception as e:
        logger.warning("safe_delete: %s", e)
        return False


# ═══════════════════════════════════════════════
# РАНГИ
# ═══════════════════════════════════════════════

def get_rank_name(percentage: float) -> str:
    """Возвращает название ранга по проценту правильных ответов."""
    for threshold, name in RANK_TABLE:
        if percentage >= threshold:
            return name
    return "🌱 Новичок"


def get_next_rank(percentage: float) -> tuple[str, int] | None:
    """Возвращает (название следующего ранга, сколько % до него) или None."""
    for i, (threshold, name) in enumerate(RANK_TABLE):
        if percentage >= threshold:
            if i == 0:
                return None  # уже максимальный ранг
            next_threshold, next_name = RANK_TABLE[i - 1]
            return next_name, next_threshold - int(percentage)
    return RANK_TABLE[-1][1], RANK_TABLE[-1][0]


# ═══════════════════════════════════════════════
# ГЕНЕРАЦИЯ КАРТИНКИ РЕЗУЛЬТАТОВ
# ═══════════════════════════════════════════════

def _find_font(bold: bool = False) -> str | None:
    """Ищет доступный шрифт в системе."""
    keyword = "Bold" if bold else "Sans"
    for path in _FONT_PATHS:
        if keyword.lower() in path.lower() or not bold:
            if os.path.exists(path):
                return path
    # Любой существующий
    for path in _FONT_PATHS:
        if os.path.exists(path):
            return path
    return None


def _load_fonts() -> dict:
    """Загружает шрифты с fallback на default."""
    try:
        from PIL import ImageFont
    except ImportError:
        return {}

    fonts = {}
    bold_path = _find_font(bold=True)
    regular_path = _find_font(bold=False)

    def _load(path, size):
        if path:
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass
        return ImageFont.load_default()

    fonts["title"] = _load(bold_path, 28)
    fonts["sub"] = _load(regular_path, 20)
    fonts["score"] = _load(bold_path, 52)
    fonts["small"] = _load(regular_path, 16)
    fonts["avatar"] = _load(bold_path, 60)

    return fonts


async def _load_avatar(bot, user_id: int, first_name: str):
    """Загружает аватарку пользователя или создаёт заглушку."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        return None

    avatar_size = (120, 120)

    # Пробуем загрузить реальную аватарку
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file_obj = await photos.photos[0][-1].get_file()
            file_bytes = await file_obj.download_as_bytearray()
            avatar_img = Image.open(io.BytesIO(bytes(file_bytes))).convert("RGBA")
            avatar_img = avatar_img.resize(avatar_size, Image.LANCZOS)

            # Круглая маска
            mask = Image.new("L", avatar_size, 0)
            ImageDraw.Draw(mask).ellipse(
                (0, 0, avatar_size[0], avatar_size[1]), fill=255
            )
            avatar_img.putalpha(mask)
            return avatar_img
    except Exception as e:
        logger.debug("Avatar load failed for %d: %s", user_id, e)

    # Заглушка — круг с инициалом
    avatar_img = Image.new("RGBA", avatar_size, _COLORS["avatar_bg"])
    draw_tmp = ImageDraw.Draw(avatar_img)
    initial = (first_name[0].upper() if first_name else "?")

    fonts = _load_fonts()
    font = fonts.get("avatar", ImageFont.load_default())

    # Центрируем букву
    try:
        bbox = draw_tmp.textbbox((0, 0), initial, font=font)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
    except Exception:
        tw, th = 40, 50
    x = (avatar_size[0] - tw) // 2
    y = (avatar_size[1] - th) // 2 - 5
    draw_tmp.text((x, y), initial, fill=_COLORS["avatar_text"], font=font)

    # Круглая маска
    mask = Image.new("L", avatar_size, 0)
    ImageDraw.Draw(mask).ellipse(
        (0, 0, avatar_size[0], avatar_size[1]), fill=255
    )
    avatar_img.putalpha(mask)

    return avatar_img


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
    Возвращает bytes или None при ошибке / отсутствии Pillow.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        logger.warning("Pillow not installed — skipping image generation")
        return None

    try:
        pct = round(score / max(total, 1) * 100)

        # Аватарка
        avatar_img = await _load_avatar(bot, user_id, first_name)

        # Шрифты
        fonts = _load_fonts()

        # Холст
        W, H = 600, 280
        img = Image.new("RGB", (W, H), _COLORS["bg_top"])
        draw = ImageDraw.Draw(img)

        # Фоновый градиент
        r1, g1, b1 = _COLORS["bg_top"]
        r2, g2, b2 = _COLORS["bg_bottom"]
        for y in range(H):
            t = y / H
            r = int(r1 + (r2 - r1) * t)
            g = int(g1 + (g2 - g1) * t)
            b = int(b1 + (b2 - b1) * t)
            draw.line([(0, y), (W, y)], fill=(r, g, b))

        # Аватарка
        if avatar_img:
            ay = (H - 120) // 2
            img.paste(avatar_img, (30, ay), avatar_img.split()[3])

        x_text = 180

        # Имя
        name_truncated = (first_name or "Игрок")[:20]
        draw.text(
            (x_text, 30), name_truncated,
            fill=_COLORS["text_name"],
            font=fonts.get("title"),
        )

        # Ранг
        # Убираем эмодзи для Pillow (они не рендерятся)
        rank_clean = re.sub(
            r'[\U00010000-\U0010ffff]', '', rank_name
        ).strip()
        draw.text(
            (x_text, 68), rank_clean or rank_name,
            fill=_COLORS["text_rank"],
            font=fonts.get("sub"),
        )

        # Счёт — крупно
        draw.text(
            (x_text, 100), f"{score}/{total}",
            fill=_COLORS["text_score"],
            font=fonts.get("score"),
        )

        # Процент
        draw.text(
            (x_text, 165), f"{pct}%  правильных ответов",
            fill=_COLORS["text_pct"],
            font=fonts.get("sub"),
        )

        # Прогресс-бар
        bar_x, bar_y = x_text, 200
        bar_w, bar_h = W - x_text - 30, 18
        draw.rounded_rectangle(
            [bar_x, bar_y, bar_x + bar_w, bar_y + bar_h],
            radius=9, fill=_COLORS["bar_bg"],
        )
        fill_w = int(bar_w * pct / 100)
        if fill_w > 0:
            if pct >= 70:
                bar_color = _COLORS["bar_good"]
            elif pct >= 50:
                bar_color = _COLORS["bar_mid"]
            else:
                bar_color = _COLORS["bar_bad"]
            draw.rounded_rectangle(
                [bar_x, bar_y, bar_x + fill_w, bar_y + bar_h],
                radius=9, fill=bar_color,
            )

        # Нижняя подпись
        draw.text(
            (x_text, 230), "Библейский тест-бот · 1 Петра",
            fill=_COLORS["text_footer"],
            font=fonts.get("small"),
        )

        # Дата
        date_str = _today_utc_display()
        draw.text(
            (W - 120, 230), date_str,
            fill=_COLORS["text_footer"],
            font=fonts.get("small"),
        )

        # Конвертируем в bytes
        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()

    except Exception as e:
        logger.error("generate_result_image error: %s", e, exc_info=True)
        return None


# ═══════════════════════════════════════════════
# ГЕНЕРАЦИЯ АНИМИРОВАННОГО GIF РЕЗУЛЬТАТОВ
# ═══════════════════════════════════════════════

async def create_result_gif(
    score: int,
    total: int,
    rank_name: str,
    time_seconds: float | None = None,
    first_name: str = "",
) -> io.BytesIO | None:
    """
    Генерирует анимированный GIF с результатами теста (20 кадров, 100ms каждый).

    Структура:
      Кадры  1–12 : анимация счётчика (0 → score), заполнение прогресс-бара
      Кадры 13–15 : fade-in ранга и деталей
      Кадры 16–20 : финальный статичный кадр

    Возвращает io.BytesIO или None при ошибке / отсутствии Pillow.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        logger.warning("Pillow not installed — skipping GIF generation")
        return None

    try:
        pct = round(score / max(total, 1) * 100)

        # ── Шрифты ───────────────────────────────────────────────────────────
        bold_path    = _find_font(bold=True)
        regular_path = _find_font(bold=False)

        def _lf(path, size):
            if path:
                try:
                    return ImageFont.truetype(path, size)
                except Exception:
                    pass
            return ImageFont.load_default()

        f_title   = _lf(bold_path,    22)
        f_score   = _lf(bold_path,    68)
        f_pct     = _lf(regular_path, 32)
        f_rank    = _lf(bold_path,    24)
        f_details = _lf(regular_path, 18)

        # ── Цвета ────────────────────────────────────────────────────────────
        C_BG       = (30,  30,  46)
        C_TITLE    = (220, 220, 255)
        C_SCORE    = (255, 215,   0)   # золотой
        C_PCT      = (180, 180, 210)
        C_BAR_BG   = (50,  50,  70)
        C_BAR_EDGE = (100, 100, 120)
        C_FLASH    = (255, 255, 255)

        if pct >= 80:
            C_BAR = (76,  175,  80)    # зелёный
        elif pct >= 50:
            C_BAR = (255, 193,   7)    # жёлтый
        else:
            C_BAR = (244,  67,  54)    # красный

        W, H = 600, 400
        BAR_X, BAR_Y = 100, 225
        BAR_W, BAR_H = 400, 28

        # Убираем эмодзи из ранга — Pillow их не рендерит
        rank_clean = re.sub(r'[\U00010000-\U0010ffff]', '', rank_name).strip()
        name_clean = re.sub(r'[\U00010000-\U0010ffff]', '', first_name).strip()

        wrong_count = total - score
        time_str = format_duration(time_seconds) if time_seconds else None

        def _draw_base(draw: ImageDraw.ImageDraw):
            """Рисует фоновый градиент и заголовок."""
            r1, g1, b1 = C_BG
            r2, g2, b2 = (50, 50, 80)
            for y in range(H):
                t = y / H
                r = int(r1 + (r2 - r1) * t)
                g = int(g1 + (g2 - g1) * t)
                b = int(b1 + (b2 - b1) * t)
                draw.line([(0, y), (W, y)], fill=(r, g, b))

            # Заголовок
            title = f"РЕЗУЛЬТАТЫ{(' • ' + name_clean[:12]) if name_clean else ''}"
            draw.text((W // 2, 30), title, fill=C_TITLE, font=f_title, anchor="mm")

        def _draw_score_and_bar(draw: ImageDraw.ImageDraw, cur_score: int, cur_pct: int):
            """Рисует текущий счёт и прогресс-бар."""
            draw.text((W // 2, 140), f"{cur_score} / {total}",
                      fill=C_SCORE, font=f_score, anchor="mm")

            # Фон бара
            draw.rectangle(
                [BAR_X, BAR_Y, BAR_X + BAR_W, BAR_Y + BAR_H],
                fill=C_BAR_BG, outline=C_BAR_EDGE,
            )
            # Заполнение
            filled = max(0, int(BAR_W * cur_pct / 100))
            if filled > 0:
                draw.rectangle(
                    [BAR_X, BAR_Y, BAR_X + filled, BAR_Y + BAR_H],
                    fill=C_BAR,
                )

            draw.text((W // 2, 275), f"{cur_pct}%",
                      fill=C_PCT, font=f_pct, anchor="mm")

        def _draw_details(draw: ImageDraw.ImageDraw, alpha: float):
            """Рисует ранг и статистику с заданной прозрачностью (0.0–1.0)."""
            def fade(base_color):
                return tuple(int(c * alpha + 30 * (1 - alpha)) for c in base_color)

            rank_color   = fade((150, 200, 255))
            detail_color = fade((170, 170, 200))

            # Ранг — чуть выше деталей
            draw.text((W // 2, 315), rank_clean or rank_name,
                      fill=rank_color, font=f_rank, anchor="mm")

            # Детали: правильные / ошибки / время
            x_left  = 130
            x_right = 470
            y_base  = 355

            draw.text((x_left,  y_base), f"✓ {score} правильных",
                      fill=detail_color, font=f_details, anchor="lm")
            draw.text((x_left,  y_base + 25), f"✗ {wrong_count} ошибок",
                      fill=detail_color, font=f_details, anchor="lm")
            if time_str:
                draw.text((x_right, y_base + 12), f"t: {time_str}",
                          fill=detail_color, font=f_details, anchor="rm")

        # ── Генерируем кадры ─────────────────────────────────────────────────
        frames: list[Image.Image] = []

        # Кадры 1–12: анимация счётчика
        for i in range(1, 13):
            img = Image.new("RGB", (W, H), C_BG)
            draw = ImageDraw.Draw(img)
            _draw_base(draw)

            cur_score = int(score * i / 12)
            cur_pct   = int(pct   * i / 12)
            _draw_score_and_bar(draw, cur_score, cur_pct)
            frames.append(img)

        # Кадр «вспышка» для хорошего результата (между 12 и 13)
        if pct >= 80:
            flash = Image.new("RGB", (W, H), C_FLASH)
            frames.append(flash)
            # Короткий: 1 кадр 60ms — добавим duration override ниже

        # Кадры 13–15: fade-in деталей
        last_base = frames[-1].copy()  # берём последний кадр как основу
        for alpha in (0.3, 0.6, 1.0):
            img = last_base.copy()
            draw = ImageDraw.Draw(img)
            # Перерисовываем финальные значения поверх
            _draw_score_and_bar(draw, score, pct)
            _draw_details(draw, alpha)
            frames.append(img)

        # Кадры 16–20: финальный статичный кадр
        final = frames[-1].copy()
        for _ in range(5):
            frames.append(final.copy())

        # ── Собираем GIF ─────────────────────────────────────────────────────
        # Длительность кадров: 100ms по умолчанию, вспышка — 60ms
        durations = [100] * len(frames)
        # Если была вспышка, она идёт после 12-го кадра (индекс 12)
        if pct >= 80 and len(frames) > 12:
            durations[12] = 60

        buf = io.BytesIO()
        frames[0].save(
            buf,
            format="GIF",
            append_images=frames[1:],
            save_all=True,
            duration=durations,
            loop=0,
            optimize=False,   # optimize=True иногда ломает анимацию в Pillow
        )
        buf.seek(0)
        return buf

    except Exception as e:
        logger.error("create_result_gif error: %s", e, exc_info=True)
        return None


# ═══════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ
# ═══════════════════════════════════════════════

def format_duration(seconds: float) -> str:
    """Форматирует продолжительность в человекочитаемый вид."""
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} сек"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} мин {secs} сек"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}ч {minutes}м"


def format_number(n: int) -> str:
    """1234 → '1 234'"""
    return f"{n:,}".replace(",", " ")


def plural_form(n: int, one: str, few: str, many: str) -> str:
    """Правильное склонение: 1 вопрос, 2 вопроса, 5 вопросов."""
    if 11 <= n % 100 <= 19:
        return f"{n} {many}"
    rem = n % 10
    if rem == 1:
        return f"{n} {one}"
    if 2 <= rem <= 4:
        return f"{n} {few}"
    return f"{n} {many}"


# ═══════════════════════════════════════════════
# GARBAGE COLLECTION
# ═══════════════════════════════════════════════

GC_STALE_THRESHOLD = 86400  # 24 часа


async def cleanup_stale_userdata(context):
    """
    JobQueue task: удаляет из user_data записи с активностью >24ч.
    Импорт bot.user_data здесь, чтобы избежать кругового импорта.
    """
    try:
        from bot import user_data
    except ImportError:
        logger.warning("Cannot import user_data for GC")
        return

    now = time.time()
    stale = [
        uid for uid, data in list(user_data.items())
        if now - data.get("last_activity", now) > GC_STALE_THRESHOLD
    ]
    for uid in stale:
        # Отменяем таймер если есть
        data = user_data.get(uid, {})
        timer = data.get("timer_task")
        if timer and not timer.done():
            timer.cancel()
        user_data.pop(uid, None)

    if stale:
        logger.info("🧹 GC: удалено %d устаревших записей user_data", len(stale))
