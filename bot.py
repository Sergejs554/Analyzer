#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, sys, json, asyncio, tempfile
from typing import Optional
from urllib.parse import quote

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BotCommand, BotCommandScopeDefault, MenuButtonCommands
)
from aiogram.filters import Command, CommandStart
import logging
logging.basicConfig(level=logging.INFO)

import aiohttp

# ============================================================
# MR MASTERING BOT -> ONLY WRAPPER
# Everything audio processing happens in Railway app.py
# /master   -> Smart Auto Mastering
# /enhance  -> Auto Enhance
# /blend    -> Blend Research / Hybrid Render
# ============================================================

# -------- ENV --------
MASTER_API_BASE = os.getenv("MASTER_API_BASE", "https://web-production-51401.up.railway.app").rstrip("/")
MAX_TG_FILE_MB = int(os.getenv("MAX_TG_FILE_MB", "19"))
MAX_TG_SEND_MB = int(os.getenv("MAX_TG_SEND_MB", "50"))
MAX_REMOTE_MB = int(os.getenv("MAX_REMOTE_MB", "256"))

ALLOWED_EXT = (".mp3", ".wav", ".m4a", ".flac", ".aiff", ".aif")

ROOT = os.path.dirname(__file__)
with open(os.path.join(ROOT, "presets.json"), "r", encoding="utf-8") as f:
    PRESETS = json.load(f)

USER_STATE = {}  # user_id -> dict

# -------- TOKEN SANITY --------
raw_token = os.getenv("BOT_TOKEN") or ""
token = (raw_token.strip()
         .replace("\ufeff", "").replace("\u200b", "")
         .replace("\u2060", "").replace("\xa0", ""))
print(f"[DEBUG] BOT_TOKEN len={len(token)} repr={repr(token)}", flush=True)
if not re.fullmatch(r"\d+:[A-Za-z0-9_\-]{35,}", token):
    print("[FATAL] Invalid BOT_TOKEN. Fix env var BOT_TOKEN.", flush=True)
    sys.exit(1)

bot = Bot(token)
dp = Dispatcher()

# -------- UI (Keyboards) --------
def label_format(fmt_key: str) -> str:
    return {
        "wav16": "WAV 16-bit",
        "mp3_320": "MP3 320",
        "wav24": "WAV 24-bit",
        "flac": "FLAC",
        "aiff": "AIFF",
    }[fmt_key]

# === изменено ===
def label_process(process_key: str) -> str:
    return {
        "master": "Smart Master",
        "enhance": "Auto Enhance",
        "blend": "Blend Research",
    }[process_key]
# === /изменено ===

def kb_main(uid: int) -> InlineKeyboardMarkup:
    st = USER_STATE.get(uid, PRESETS["defaults"])
    # === изменено ===
    process = st.get("process", "master")
    # === /изменено ===
    return InlineKeyboardMarkup(inline_keyboard=[
        # === изменено ===
        [InlineKeyboardButton(text=f"✨ Mode: {label_process(process)}", callback_data="toggle_process")],
        # === /изменено ===
        [InlineKeyboardButton(text=f"🎚 Intensity: {st['intensity']}", callback_data="menu_intensity")],
        [InlineKeyboardButton(text=f"🎛 Tone: {st['tone']}", callback_data="menu_tone")],
        [InlineKeyboardButton(text=f"💾 Output: {label_format(st['format'])}", callback_data="menu_format")],
        [InlineKeyboardButton(text="✅ Smart Auto", callback_data="noop_auto")],
    ])

def kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

def kb_intensity() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Low", callback_data="set_intensity_low"),
         InlineKeyboardButton(text="Balanced", callback_data="set_intensity_balanced"),
         InlineKeyboardButton(text="High", callback_data="set_intensity_high")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main"),
         InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

def kb_tone() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Warm", callback_data="set_tone_warm"),
         InlineKeyboardButton(text="Balanced", callback_data="set_tone_balanced"),
         InlineKeyboardButton(text="Bright", callback_data="set_tone_bright")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main"),
         InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

def kb_format() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="WAV 16-bit", callback_data="set_fmt_wav16")],
        [InlineKeyboardButton(text="MP3 320 kbps", callback_data="set_fmt_mp3_320")],
        [InlineKeyboardButton(text="Ultra HD WAV 24-bit", callback_data="set_fmt_wav24")],
        [InlineKeyboardButton(text="FLAC", callback_data="set_fmt_flac")],
        [InlineKeyboardButton(text="AIFF", callback_data="set_fmt_aiff")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main"),
         InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

# -------- COMMAND MENU --------
async def setup_menu():
    await bot.set_my_commands(
        commands=[
            BotCommand(command="start", description="Главное меню"),
            BotCommand(command="menu", description="Показать меню"),
            BotCommand(command="settings", description="Сброс/настройки"),
        ],
        scope=BotCommandScopeDefault()
    )
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())

@dp.message(CommandStart())
async def start(m: Message):
    USER_STATE[m.from_user.id] = {
        "intensity": PRESETS["defaults"]["intensity"],
        "tone": PRESETS["defaults"]["tone"],
        "format": PRESETS["defaults"]["format"],
        # === изменено ===
        "process": "master",
        # === /изменено ===
    }
    await m.answer(
        "👋 Привет! Я — Mr. Mastering.\n"
        "Пришли аудио-файл (.mp3/.m4a/.wav/.flac/.aiff) до ~19 MB или ссылку (Google Drive/прямая).\n"
        "Выбери Tone + Intensity + Output.\n"
        "Smart Auto всегда включён (под капотом).",
        reply_markup=kb_main(m.from_user.id)
    )

@dp.message(Command("menu"))
async def menu_cmd(m: Message):
    await m.answer("Главное меню:", reply_markup=kb_main(m.from_user.id))

@dp.message(Command("settings"))
async def settings_cmd(m: Message):
    USER_STATE[m.from_user.id] = {
        "intensity": PRESETS["defaults"]["intensity"],
        "tone": PRESETS["defaults"]["tone"],
        "format": PRESETS["defaults"]["format"],
        # === изменено ===
        "process": "master",
        # === /изменено ===
    }
    await m.answer("⚙️ Настройки сброшены.", reply_markup=kb_main(m.from_user.id))

# -------- CALLBACKS --------
@dp.callback_query()
async def callbacks(c):
    uid = c.from_user.id
    data = c.data
    st = USER_STATE.get(uid, PRESETS["defaults"])

    if data == "noop_auto":
        await c.answer("Smart Auto всегда включён.")
        return

    if data in ("go_home", "back_main"):
        await c.message.edit_text("Главное меню:", reply_markup=kb_main(uid))
        await c.answer()
        return

    # === изменено ===
    if data == "toggle_process":
        current = st.get("process", "master")
        if current == "master":
            st["process"] = "enhance"
        elif current == "enhance":
            st["process"] = "blend"
        else:
            st["process"] = "master"

        await c.message.edit_text(
            f"Режим обработки: {label_process(st['process'])}",
            reply_markup=kb_main(uid)
        )
        await c.answer()
        return
    # === /изменено ===

    if data == "menu_intensity":
        await c.message.edit_text("Выбери интенсивность мастеринга:", reply_markup=kb_intensity())
    elif data == "menu_tone":
        await c.message.edit_text("Выбери тон (тембральный баланс):", reply_markup=kb_tone())
    elif data == "menu_format":
        await c.message.edit_text("Выбери формат итогового файла:", reply_markup=kb_format())
    elif data.startswith("set_intensity_"):
        intensity = data.split("set_intensity_")[1]
        st["intensity"] = intensity
        await c.message.edit_text(f"Интенсивность: {intensity}", reply_markup=kb_main(uid))
    elif data.startswith("set_tone_"):
        tone = data.split("set_tone_")[1]
        st["tone"] = tone
        await c.message.edit_text(f"Тон: {tone}", reply_markup=kb_main(uid))
    elif data.startswith("set_fmt_"):
        fmt = data.split("set_fmt_")[1]
        st["format"] = fmt
        await c.message.edit_text(f"Формат результата: {label_format(fmt)}", reply_markup=kb_main(uid))

    await c.answer()

# -------- HELPERS --------
def _too_big(bytes_size: int, mb: int) -> bool:
    return bytes_size > mb * 1024 * 1024

GDRIVE_RX = re.compile(r"(?:https?://)?(?:drive\.google\.com)/(?:file/d/|open\?id=|uc\?id=)([\w-]+)")
DIRECT_RX = re.compile(r"^https?://", re.IGNORECASE)

def is_gdrive(url: str) -> bool:
    return GDRIVE_RX.search(url or "") is not None

def gdrive_direct(url: str) -> Optional[str]:
    m = GDRIVE_RX.search(url or "")
    if not m:
        return None
    file_id = m.group(1)
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def _norm_tone(x: str) -> str:
    x = (x or "balanced").lower().strip()
    return x if x in ("warm", "balanced", "bright") else "balanced"

def _norm_intensity(x: str) -> str:
    x = (x or "balanced").lower().strip()
    if x in ("soft",):
        return "low"
    if x in ("normal",):
        return "balanced"
    if x in ("hard",):
        return "high"
    return x if x in ("low", "balanced", "high") else "balanced"

def _norm_format(x: str) -> str:
    x = (x or "wav16").lower().strip()
    if x in ("wav", "wav16"):
        return "wav16"
    if x in ("wav24",):
        return "wav24"
    if x in ("mp3", "mp3_320"):
        return "mp3_320"
    if x in ("flac",):
        return "flac"
    if x in ("aiff", "aif"):
        return "aiff"
    return "wav16"

# === изменено ===
def _norm_process(x: str) -> str:
    x = (x or "master").lower().strip()
    return x if x in ("master", "enhance", "blend") else "master"
# === /изменено ===

def _api_master_url(file_url: str, tone: str, intensity: str, fmt: str) -> str:
    fu = quote(file_url, safe="")
    return f"{MASTER_API_BASE}/master?file={fu}&tone={tone}&intensity={intensity}&format={fmt}"

# === изменено ===
def _api_enhance_url(file_url: str, fmt: str) -> str:
    fu = quote(file_url, safe="")
    return f"{MASTER_API_BASE}/enhance?file={fu}&format={fmt}"

def _api_blend_url(file_url: str, tone: str, intensity: str, fmt: str) -> str:
    fu = quote(file_url, safe="")
    return f"{MASTER_API_BASE}/blend?file={fu}&tone={tone}&intensity={intensity}&format={fmt}"
# === /изменено ===

async def _download_to_file(session: aiohttp.ClientSession, url: str, dst_path: str, max_mb: int = 256):
    total = 0
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=900)) as r:
        r.raise_for_status()
        with open(dst_path, "wb") as f:
            async for chunk in r.content.iter_chunked(1 << 16):
                if not chunk:
                    break
                total += len(chunk)
                if total > max_mb * 1024 * 1024:
                    raise RuntimeError("Remote file too big")
                f.write(chunk)

async def _master_via_api(session: aiohttp.ClientSession, file_url: str, tone: str, intensity: str, fmt: str, out_path: str):
    url = _api_master_url(file_url, tone, intensity, fmt)
    await _download_to_file(session, url, out_path, max_mb=MAX_REMOTE_MB)

# === изменено ===
async def _enhance_via_api(session: aiohttp.ClientSession, file_url: str, fmt: str, out_path: str):
    url = _api_enhance_url(file_url, fmt)
    await _download_to_file(session, url, out_path, max_mb=MAX_REMOTE_MB)

async def _blend_via_api(session: aiohttp.ClientSession, file_url: str, tone: str, intensity: str, fmt: str, out_path: str):
    url = _api_blend_url(file_url, tone, intensity, fmt)
    await _download_to_file(session, url, out_path, max_mb=MAX_REMOTE_MB)

async def _process_via_api(
    session: aiohttp.ClientSession,
    process_mode: str,
    file_url: str,
    tone: str,
    intensity: str,
    fmt: str,
    out_path: str
):
    if process_mode == "enhance":
        await _enhance_via_api(session, file_url, fmt, out_path)
    elif process_mode == "blend":
        await _blend_via_api(session, file_url, tone, intensity, fmt, out_path)
    else:
        await _master_via_api(session, file_url, tone, intensity, fmt, out_path)
# === /изменено ===

def _guess_filename(fmt: str) -> str:
    if fmt == "mp3_320":
        return "mastered_320.mp3"
    if fmt == "wav24":
        return "mastered_uhd.wav"
    if fmt == "flac":
        return "mastered.flac"
    if fmt == "aiff":
        return "mastered.aiff"
    return "mastered.wav"

def _fallback_notice(fmt: str) -> str:
    return (
        f"⚠️ {label_format(fmt)} готов, но Telegram не пропускает файл по размеру.\n"
        f"Ниже отправляю MP3 320 kbps для Telegram."
    )

async def _telegram_file_direct_url(file_id: str) -> str:
    fi = await bot.get_file(file_id)
    return f"https://api.telegram.org/file/bot{token}/{fi.file_path}"

# -------- HANDLERS --------
@dp.message(F.audio | F.document)
async def on_audio(m: Message):
    file_obj = m.audio or m.document
    if not file_obj:
        return

    name = (file_obj.file_name or "input").lower()
    if not name.endswith(ALLOWED_EXT):
        await m.reply("⚠️ Пришли аудио с расширением .mp3/.m4a/.wav/.flac/.aiff", reply_markup=kb_home())
        return

    size = file_obj.file_size or 0
    if _too_big(size, MAX_TG_FILE_MB):
        await m.reply(
            f"⚠️ Файл **{round(size/1024/1024, 1)} MB** слишком большой для Telegram.\n"
            f"Отправь **ссылку** (Google Drive/прямая), и я сделаю мастеринг через API.",
            reply_markup=kb_home()
        )
        return

    st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
    tone = _norm_tone(st.get("tone", "balanced"))
    intensity = _norm_intensity(st.get("intensity", "balanced"))
    fmt = _norm_format(st.get("format", "wav16"))
    # === изменено ===
    process_mode = _norm_process(st.get("process", "master"))
    if process_mode == "enhance":
        action_text = "🎧 Файл получен. Энхансю через API…"
    elif process_mode == "blend":
        action_text = "🎧 Файл получен. Делаю blend-рендер через API…"
    else:
        action_text = "🎧 Файл получен. Мастерю через API…"
    # === /изменено ===

    await m.reply(action_text, reply_markup=kb_home())

    try:
        with tempfile.TemporaryDirectory() as td:
            out_name = _guess_filename(fmt)
            out_path = os.path.join(td, out_name)

            src_url = await _telegram_file_direct_url(file_obj.file_id)

            async with aiohttp.ClientSession() as session:
                # === изменено ===
                await _process_via_api(session, process_mode, src_url, tone, intensity, fmt, out_path)
                # === /изменено ===

            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                # === изменено ===
                if fmt != "mp3_320":
                    await m.reply(_fallback_notice(fmt), reply_markup=kb_home())

                alt_name = _guess_filename("mp3_320")
                alt_path = os.path.join(td, alt_name)
                async with aiohttp.ClientSession() as session:
                    await _process_via_api(session, process_mode, src_url, tone, intensity, "mp3_320", alt_path)

                done_label = label_process(process_mode)
                await m.reply_document(
                    FSInputFile(alt_path, filename=alt_name),
                    caption=(
                        f"✅ Готово! Результат: MP3 320 kbps\nРежим: {done_label}"
                        if fmt == "mp3_320"
                        else f"✅ Готово! Telegram-версия: MP3 320 kbps\nРежим: {done_label}\nВыбранный формат: {label_format(fmt)}"
                    ),
                    reply_markup=kb_home()
                )
            else:
                done_label = label_process(process_mode)
                await m.reply_document(
                    FSInputFile(out_path, filename=out_name),
                    caption=f"✅ Готово! Результат: {label_format(fmt)}\nРежим: {done_label}",
                    reply_markup=kb_home()
                )
                # === /изменено ===

    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}", reply_markup=kb_home())

@dp.message(F.text)
async def on_text(m: Message):
    url = (m.text or "").strip()
    if not url:
        return
    if not (is_gdrive(url) or DIRECT_RX.match(url)):
        return

    st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
    tone = _norm_tone(st.get("tone", "balanced"))
    intensity = _norm_intensity(st.get("intensity", "balanced"))
    fmt = _norm_format(st.get("format", "wav16"))
    # === изменено ===
    process_mode = _norm_process(st.get("process", "master"))
    if process_mode == "enhance":
        action_text = "⏬ Скачиваю по ссылке и энхансю через API…"
    elif process_mode == "blend":
        action_text = "⏬ Скачиваю по ссылке и делаю blend-рендер через API…"
    else:
        action_text = "⏬ Скачиваю по ссылке и мастерю через API…"
    # === /изменено ===

    await m.reply(action_text, reply_markup=kb_home())

    try:
        with tempfile.TemporaryDirectory() as td:
            if is_gdrive(url):
                url = gdrive_direct(url) or url

            out_name = _guess_filename(fmt)
            out_path = os.path.join(td, out_name)

            async with aiohttp.ClientSession() as session:
                # === изменено ===
                await _process_via_api(session, process_mode, url, tone, intensity, fmt, out_path)
                # === /изменено ===

            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                # === изменено ===
                if fmt != "mp3_320":
                    await m.reply(_fallback_notice(fmt), reply_markup=kb_home())

                alt_name = _guess_filename("mp3_320")
                alt_path = os.path.join(td, alt_name)
                async with aiohttp.ClientSession() as session:
                    await _process_via_api(session, process_mode, url, tone, intensity, "mp3_320", alt_path)

                done_label = label_process(process_mode)
                await m.reply_document(
                    FSInputFile(alt_path, filename=alt_name),
                    caption=(
                        f"✅ Готово! Результат: MP3 320 kbps\nРежим: {done_label}"
                        if fmt == "mp3_320"
                        else f"✅ Готово! Telegram-версия: MP3 320 kbps\nРежим: {done_label}\nВыбранный формат: {label_format(fmt)}"
                    ),
                    reply_markup=kb_home()
                )
            else:
                done_label = label_process(process_mode)
                await m.reply_document(
                    FSInputFile(out_path, filename=out_name),
                    caption=f"✅ Готово! Результат: {label_format(fmt)}\nРежим: {done_label}",
                    reply_markup=kb_home()
                )
                # === /изменено ===

    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}", reply_markup=kb_home())

# -------- MAIN --------
async def _runner():
    await bot.delete_webhook(drop_pending_updates=True)
    await setup_menu()
    print(f"Mr Mastering bot is running… MASTER_API_BASE={MASTER_API_BASE}", flush=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

def main():
    asyncio.run(_runner())

if __name__ == "__main__":
    main()
