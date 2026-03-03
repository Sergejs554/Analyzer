#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, sys, shlex, json, asyncio, tempfile
from typing import Optional
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BotCommand, BotCommandScopeDefault, MenuButtonCommands
)
from aiogram.filters import Command, CommandStart
import logging
logging.basicConfig(level=logging.INFO)

import aiohttp
import numpy as np

# === изменено ===
# Используем наш прод-мастеринг (как в app.py): analyze_sections + decide_smart_params_with_sections + AIR BUS + loudnorm 2-pass
from auto_analysis import analyze_file, analyze_sections
from smart_auto import decide_smart_params_with_sections, build_smart_chain

# -------- TOKEN SANITY --------
raw_token = os.getenv("BOT_TOKEN") or ""
token = (raw_token.strip()
         .replace("\ufeff","").replace("\u200b","")
         .replace("\u2060","").replace("\xa0",""))
print(f"[DEBUG] BOT_TOKEN len={len(token)} repr={repr(token)}", flush=True)
if not re.fullmatch(r"\d+:[A-Za-z0-9_\-]{35,}", token):
    print("[FATAL] Invalid BOT_TOKEN. Fix env var BOT_TOKEN.", flush=True)
    sys.exit(1)

bot = Bot(token)
dp = Dispatcher()

# -------- SETTINGS --------
MAX_TG_FILE_MB = int(os.getenv("MAX_TG_FILE_MB", "19"))
MAX_TG_SEND_MB = int(os.getenv("MAX_TG_SEND_MB", "49"))
ALLOWED_EXT = (".mp3", ".wav", ".m4a", ".flac", ".aiff", ".aif")  # === изменено === add flac/aiff
VERBOSE = os.getenv("VERBOSE_ANALYSIS", "0") == "1"

ROOT = os.path.dirname(__file__)
with open(os.path.join(ROOT, "presets.json"), "r", encoding="utf-8") as f:
    PRESETS = json.load(f)

USER_STATE = {}  # user_id -> dict

# -------- UI (Keyboards) --------
def label_format(fmt_key: str) -> str:
    # === изменено ===
    return {
        "wav16": "WAV 16-bit",
        "mp3_320": "MP3 320",
        "wav24": "WAV 24-bit",
        "flac": "FLAC",
        "aiff": "AIFF"
    }[fmt_key]

def kb_main(uid):
    st = USER_STATE.get(uid, PRESETS["defaults"])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎚 Intensity: {st['intensity']}", callback_data="menu_intensity")],
        [InlineKeyboardButton(text=f"🎛 Tone: {st['tone']}", callback_data="menu_tone")],
        [InlineKeyboardButton(text=f"💾 Output: {label_format(st['format'])}", callback_data="menu_format")],
        # === изменено ===
        # Smart Auto всегда включён. Кнопку оставляем как статус, без переключения.
        [InlineKeyboardButton(text="✅ Smart Auto", callback_data="noop_auto")]
    ])

def kb_home():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

def kb_intensity():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Low", callback_data="set_intensity_low"),
         InlineKeyboardButton(text="Balanced", callback_data="set_intensity_balanced"),
         InlineKeyboardButton(text="High", callback_data="set_intensity_high")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main"),
         InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

def kb_tone():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Warm", callback_data="set_tone_warm"),
         InlineKeyboardButton(text="Balanced", callback_data="set_tone_balanced"),
         InlineKeyboardButton(text="Bright", callback_data="set_tone_bright")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main"),
         InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

def kb_format():
    # === изменено ===
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="WAV 16-bit", callback_data="set_fmt_wav16")],
        [InlineKeyboardButton(text="MP3 320 kbps", callback_data="set_fmt_mp3_320")],
        [InlineKeyboardButton(text="Ultra HD WAV 24-bit", callback_data="set_fmt_wav24")],
        [InlineKeyboardButton(text="FLAC", callback_data="set_fmt_flac")],
        [InlineKeyboardButton(text="AIFF", callback_data="set_fmt_aiff")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main"),
         InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

# -------- COMMAND MENU (persist кнопка-меню) --------
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
    # === изменено ===
    # Smart Auto всегда ON, не сохраняем "auto" как переключатель.
    USER_STATE[m.from_user.id] = {
        "intensity": PRESETS["defaults"]["intensity"],
        "tone": PRESETS["defaults"]["tone"],
        "format": PRESETS["defaults"]["format"],
    }
    await m.answer(
        "👋 Привет! Я — Mr. Mastering.\n"
        "Пришли аудио-файл **.mp3**, **.m4a** или **.wav** (до ~19 MB), либо **ссылку** на файл (Google Drive/прямая ссылка).\n"
        "Параметры: Tone + Intensity + Output.\n"
        "Smart Auto всегда включён.",
        reply_markup=kb_main(m.from_user.id)
    )

@dp.message(Command("menu"))
async def menu_cmd(m: Message):
    await m.answer("Главное меню:", reply_markup=kb_main(m.from_user.id))

@dp.message(Command("settings"))
async def settings_cmd(m: Message):
    # === изменено ===
    USER_STATE[m.from_user.id] = {
        "intensity": PRESETS["defaults"]["intensity"],
        "tone": PRESETS["defaults"]["tone"],
        "format": PRESETS["defaults"]["format"],
    }
    await m.answer("⚙️ Настройки сброшены.", reply_markup=kb_main(m.from_user.id))

# -------- CALLBACK HANDLERS: INLINE MENU --------
@dp.callback_query()
async def callbacks(c):
    uid = c.from_user.id
    data = c.data
    st = USER_STATE.get(uid, PRESETS["defaults"])

    if data in ("noop_auto",):
        await c.answer("Smart Auto всегда включён.")
        return

    if data == "go_home" or data == "back_main":
        await c.message.edit_text("Главное меню:", reply_markup=kb_main(uid))
        await c.answer()
        return

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

# ---------------------------
# MR MASTERING v2 — BOT DSP
# ---------------------------

# === изменено ===
# Fixed Pre-Clean (НЕ зависит от tone/intensity, НЕ зависит от section mapping)
_PRE_CLEAN_CHAIN = "highpass=f=25:width=0.7,afftdn=nf=-25"

# === изменено ===
# AIR BUS (как в app.py): добавляет воздушность без нарезки/склеек
_AIR_AMOUNT = 0.16
_AIR_SHELF_F = 9000
_AIR_SHELF_G = 2.6
_AIR_WIDEN = 0.12  # 0..1 -> delay=1..100

_RAMP_MIN = 0.08
_RAMP_MAX = 0.80

def _clamp(x, lo, hi):
    return float(max(lo, min(hi, x)))

def _stereowiden_filter() -> str:
    d = int(round(_clamp(_AIR_WIDEN, 0.0, 1.0) * 100.0))
    d = max(1, min(100, d))
    return f"stereowiden=delay={d}"

def _pick_ramp(prev_len: float, next_len: float) -> float:
    r = min(_RAMP_MAX, 0.25 * prev_len, 0.25 * next_len)
    return _clamp(r, _RAMP_MIN, _RAMP_MAX)

def _build_mask_expr_from_sections(sections: list[dict]) -> str:
    if not sections:
        return "0.5"

    secs = sorted(sections, key=lambda s: float(s.get("start", 0.0)))
    starts = [float(s.get("start", 0.0)) for s in secs]
    ends = [float(s.get("end", 0.0)) for s in secs]
    w = [_clamp(float(s.get("level", 0.5)), 0.0, 1.0) for s in secs]

    expr = f"{w[-1]:.6f}"
    for i in range(len(w) - 2, -1, -1):
        b = max(starts[i+1], ends[i])
        prev_len = max(0.01, ends[i] - starts[i])
        next_len = max(0.01, ends[i+1] - starts[i+1])
        r = _pick_ramp(prev_len, next_len)

        left = b - r
        right = b + r
        wi = w[i]
        wj = w[i+1]

        expr = (
            f"if(lt(t,{left:.6f}),{wi:.6f},"
            f"if(lt(t,{right:.6f}),"
            f"({wi:.6f}+({wj:.6f}-{wi:.6f})*(t-{left:.6f})/{(2*r):.6f}),"
            f"({expr})"
            f"))"
        )
    return expr

def _strip_loudnorm(chain: str) -> tuple[str, str]:
    if "loudnorm=" not in chain:
        return chain, ""
    pre, ln = chain.rsplit("loudnorm=", 1)
    pre = pre.rstrip(",")
    ln = "loudnorm=" + ln
    return pre, ln

async def _run_shell(cmd: str):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError((err or b"").decode("utf-8", errors="ignore")[:4000])
    return (out or b"").decode("utf-8", errors="ignore"), (err or b"").decode("utf-8", errors="ignore")

def output_args(fmt_key: str):
    # === изменено ===
    fmt_key = (fmt_key or "wav16").lower()
    if fmt_key == "wav16":
        return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"
    if fmt_key == "wav24":
        return "-ar 48000 -ac 2 -c:a pcm_s24le", "mastered_uhd.wav"
    if fmt_key == "mp3_320":
        return "-ar 48000 -ac 2 -c:a libmp3lame -b:a 320k", "mastered_320.mp3"
    if fmt_key == "flac":
        return "-ar 48000 -ac 2 -c:a flac", "mastered.flac"
    if fmt_key == "aiff":
        return "-ar 48000 -ac 2 -f aiff -c:a pcm_s16be", "mastered.aiff"
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"

def _too_big(bytes_size: int, mb: int) -> bool:
    return bytes_size > mb * 1024 * 1024

# -------- LINK HELPERS (Drive / direct) --------
GDRIVE_RX = re.compile(r"(?:https?://)?(?:drive\.google\.com)/(?:file/d/|open\?id=|uc\?id=)([\w-]+)")
DIRECT_RX = re.compile(r"^https?://.*\.(mp3|wav|m4a|flac|aiff|aif)(\?.*)?$", re.IGNORECASE)

def is_gdrive(url: str) -> bool:
    return GDRIVE_RX.search(url or "") is not None

def gdrive_direct(url: str) -> Optional[str]:
    m = GDRIVE_RX.search(url or "")
    if not m:
        return None
    file_id = m.group(1)
    return f"https://drive.google.com/uc?export=download&id={file_id}"

async def http_download(session: aiohttp.ClientSession, url: str, dst_path: str, max_mb: int = 256) -> int:
    total = 0
    async with session.get(url, timeout=120) as r:
        r.raise_for_status()
        with open(dst_path, "wb") as f:
            async for chunk in r.content.iter_chunked(1 << 14):
                if not chunk:
                    break
                total += len(chunk)
                if total > max_mb * 1024 * 1024:
                    raise RuntimeError("Remote file too big")
                f.write(chunk)
    return total

# -------- LOUDNORM UTILS (Two-Pass Implementation) --------
def _force_print_format_json(chain: str) -> str:
    if "loudnorm=" not in chain:
        return chain
    parts = chain.rsplit("loudnorm=", 1)
    tail = parts[-1]
    if "print_format=" in tail:
        tail = re.sub(r"(print_format=)(\w+)", r"\1json", tail, count=1)
    else:
        tail += ":print_format=json"
    return "loudnorm=".join(parts[:-1] + [tail])

def _extract_last_json_block(text: str) -> Optional[dict]:
    start = -1
    depth = 0
    last_obj = None
    for i, ch in enumerate(text):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            if depth > 0:
                depth -= 1
                if depth == 0 and start != -1:
                    chunk = text[start:i+1]
                    try:
                        last_obj = json.loads(chunk)
                    except Exception:
                        pass
                    start = -1
    return last_obj

def _extract_loudnorm_targets(chain: str):
    mI  = re.search(r"loudnorm=[^,]*\bI=([-\d.]+)", chain)
    mTP = re.search(r"loudnorm=[^,]*\bTP=([-\d.]+)", chain)
    mLRA = re.search(r"loudnorm=[^,]*\bLRA=([-\d.]+)", chain)
    try:
        I = float(mI.group(1)) if mI else -14.0
    except Exception:
        I = -14.0
    try:
        TP = float(mTP.group(1)) if mTP else -1.2
    except Exception:
        TP = -1.2
    try:
        LRA = float(mLRA.group(1)) if mLRA else 7.0
    except Exception:
        LRA = 7.0
    TP = float(np.clip(TP, -9.0, 0.0))
    return I, TP, LRA

async def ffmpeg_loudnorm_two_pass(in_path: str, af_chain: str, out_args: str, out_path: str):
    if "loudnorm" not in af_chain:
        cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{af_chain}" {out_args} {shlex.quote(out_path)}'
        await _run_shell(cmd)
        return

    pass1_chain = _force_print_format_json(af_chain)
    pass1_cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{pass1_chain}" -f null -'
    _, err1 = await _run_shell(pass1_cmd)
    stats = _extract_last_json_block(err1)

    target_I, target_TP, target_LRA = _extract_loudnorm_targets(af_chain)
    if stats:
        measured_args = (
            f"I={target_I}:TP={target_TP}:LRA={target_LRA}:"
            f"measured_I={stats.get('input_i', '-14')}:"
            f"measured_LRA={stats.get('input_lra', '7')}:"
            f"measured_TP={stats.get('input_tp', '-2')}:"
            f"measured_thresh={stats.get('input_thresh', '-24')}:"
            f"offset={stats.get('target_offset', '0')}:print_format=summary"
        )
        prefix, _ = af_chain.rsplit("loudnorm=", 1)
        af_chain = prefix + "loudnorm=" + measured_args

    pass2_cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{af_chain}" {out_args} {shlex.quote(out_path)}'
    await _run_shell(pass2_cmd)

# === изменено ===
async def _render_master_v2(in_path: str, tone: str, intensity: str, fmt_key: str, td: str) -> tuple[str, str]:
    """
    Наш текущий v2 (как в app.py):
      1) analyze_sections -> sections(level) для mask(t)
      2) decide_smart_params_with_sections -> берём base_params (одна цепь на весь трек)
      3) рендер base.wav (preclean + chain без loudnorm)
      4) AIR BUS (маска по секциям) -> mixed.wav
      5) loudnorm 2-pass один раз в конце -> output
    """
    sec = analyze_sections(in_path, target_sr=48000)
    global_a = sec["global"]
    sections = sec.get("sections") or []

    tone = (tone or "balanced").lower().strip()
    intensity = (intensity or "balanced").lower().strip()
    if tone not in ("warm", "balanced", "bright"):
        tone = "balanced"
    if intensity not in ("low", "balanced", "high"):
        intensity = "balanced"

    sp = decide_smart_params_with_sections(
        global_analysis=global_a,
        sections=sections,
        intensity=intensity,
        tone_mode=tone
    )
    base_params = sp["base_params"]

    # base chain WITHOUT loudnorm
    base_chain = build_smart_chain(base_params)
    base_no_ln, _ = _strip_loudnorm(base_chain)

    base_wav = os.path.join(td, "base.wav")
    mixed_wav = os.path.join(td, "mixed.wav")

    # render base
    cmd_base = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN},{base_no_ln}" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(base_wav)}'
    )
    await _run_shell(cmd_base)

    # AIR BUS apply
    mask_expr = _build_mask_expr_from_sections(sections)
    air_gain_expr = f"(({mask_expr})*{_AIR_AMOUNT:.6f})"
    fc = (
        f"[0:a]asplit=2[dry][air];"
        f"[dry]volume=1[d0];"
        f"[air]"
        f"highshelf=f={_AIR_SHELF_F}:g={_AIR_SHELF_G},"
        f"{_stereowiden_filter()},"
        f"volume='{air_gain_expr}':eval=frame[a1];"
        f"[d0][a1]amix=inputs=2:normalize=0[aout]"
    )
    cmd_air = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(base_wav)} '
        f'-filter_complex "{fc}" -map "[aout]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(mixed_wav)}'
    )
    await _run_shell(cmd_air)

    # final loudnorm 2-pass (one time)
    out_args, out_name = output_args(fmt_key)
    out_path = os.path.join(td, out_name)

    ln = base_params["loudnorm"]
    ln_chain = f"loudnorm=I={float(ln['I'])}:TP={float(ln['TP'])}:LRA={float(ln['LRA'])}:print_format=summary"
    await ffmpeg_loudnorm_two_pass(mixed_wav, ln_chain, out_args, out_path)

    return out_path, out_name

# -------- CORE HANDLER: AUDIO FILES --------
@dp.message(F.audio | F.document)
async def on_audio(m: Message):
    file_obj = m.audio or m.document
    if not file_obj:
        return

    name = (file_obj.file_name or "input").lower()
    if not name.endswith(tuple(ALLOWED_EXT)):
        await m.reply("⚠️ Пожалуйста, пришли аудио-файл **.mp3**, **.m4a** или **.wav**.", reply_markup=kb_home())
        return

    size = file_obj.file_size or 0
    if _too_big(size, MAX_TG_FILE_MB):
        await m.reply(
            f"⚠️ Файл **{round(size/1024/1024, 1)} MB** слишком большой для Telegram.\n"
            f"Отправь **ссылку** на файл (Google Drive или прямая ссылка), я скачаю и сделаю мастеринг.",
            reply_markup=kb_home()
        )
        return

    st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
    inten_key = st.get("intensity", "balanced")
    tone_key = st.get("tone", "balanced")
    fmt_key = st.get("format", "wav16")

    await m.reply("🎧 Файл получен. Мастерю…", reply_markup=kb_home())

    try:
        with tempfile.TemporaryDirectory() as td:
            in_path = os.path.join(td, name)

            file_info = await bot.get_file(file_obj.file_id)
            await bot.download_file(file_info.file_path, in_path)

            out_path, out_name = await _render_master_v2(in_path, tone=tone_key, intensity=inten_key, fmt_key=fmt_key, td=td)

            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                # fallback to mp3_320
                alt_args, alt_name = output_args("mp3_320")
                alt_path = os.path.join(td, alt_name)

                # просто перекодируем готовый WAV/FLAC/AIFF -> mp3
                cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(out_path)} {alt_args} {shlex.quote(alt_path)}'
                await _run_shell(cmd)

                await m.reply_document(
                    FSInputFile(alt_path, filename=alt_name),
                    caption=(f"✅ Готово! Результат: MP3 320 kbps\n"
                             f"(большой файл > {MAX_TG_SEND_MB} MB, поэтому отправлен MP3)"),
                    reply_markup=kb_home()
                )
            else:
                await m.reply_document(
                    FSInputFile(out_path, filename=out_name),
                    caption=f"✅ Готово! Результат: {label_format(fmt_key)}",
                    reply_markup=kb_home()
                )

    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}", reply_markup=kb_home())

# -------- CORE HANDLER: LINK (Google Drive / direct) --------
@dp.message(F.text)
async def on_text(m: Message):
    url = (m.text or "").strip()
    if not (is_gdrive(url) or DIRECT_RX.match(url)):
        return

    await m.reply("⏬ Скачиваю файл по ссылке, выполняю мастеринг…", reply_markup=kb_home())

    try:
        with tempfile.TemporaryDirectory() as td:
            if is_gdrive(url):
                url = gdrive_direct(url) or url

            # ext guess
            lu = url.lower()
            if ".mp3" in lu:
                ext = ".mp3"
            elif ".m4a" in lu:
                ext = ".m4a"
            elif ".flac" in lu:
                ext = ".flac"
            elif ".aiff" in lu or ".aif" in lu:
                ext = ".aiff"
            else:
                ext = ".wav"

            in_path = os.path.join(td, f"input_from_link{ext}")
            async with aiohttp.ClientSession() as session:
                await http_download(session, url, in_path, max_mb=256)

            st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
            inten_key = st.get("intensity", "balanced")
            tone_key = st.get("tone", "balanced")
            fmt_key = st.get("format", "wav16")

            out_path, out_name = await _render_master_v2(in_path, tone=tone_key, intensity=inten_key, fmt_key=fmt_key, td=td)

            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                alt_args, alt_name = output_args("mp3_320")
                alt_path = os.path.join(td, alt_name)
                cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(out_path)} {alt_args} {shlex.quote(alt_path)}'
                await _run_shell(cmd)

                await m.reply_document(
                    FSInputFile(alt_path, filename=alt_name),
                    caption=(f"✅ Готово! Результат: MP3 320 kbps\n"
                             f"(большой файл > {MAX_TG_SEND_MB} MB, поэтому отправлен MP3)"),
                    reply_markup=kb_home()
                )
            else:
                await m.reply_document(
                    FSInputFile(out_path, filename=out_name),
                    caption=f"✅ Готово! Результат: {label_format(fmt_key)}",
                    reply_markup=kb_home()
                )

    except Exception as e:
        await m.reply(f"❌ Ошибка при загрузке/мастеринге: {e}", reply_markup=kb_home())

# -------- MAIN --------
async def _runner():
    await bot.delete_webhook(drop_pending_updates=True)
    await setup_menu()
    print("Mr Mastering bot is running…", flush=True)
    await dp.start_polling(
        bot,
        allowed_updates=dp.resolve_used_update_types()
    )

def main():
    asyncio.run(_runner())

if __name__ == "__main__":
    main()
