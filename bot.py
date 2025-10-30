#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, sys, shlex, json, asyncio, tempfile
from typing import Optional
from smart_auto import decide_smart_params, build_smart_chain
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BotCommand, BotCommandScopeDefault, MenuButtonCommands
)
from aiogram.filters import Command, CommandStart  # === добавлено ===
import logging
logging.basicConfig(level=logging.INFO)

import aiohttp

import numpy as np
import librosa, pyloudnorm as pyln

# === changed ===
# Import the new analysis and smart auto modules
from auto_analysis import analyze_file  # full analysis (LUFS, TP, LRA, tilt, etc.)
from smart_auto import decide_smart_params, build_smart_chain  # smart auto logic

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
ALLOWED_EXT = (".mp3", ".wav", ".m4a")  # === changed === added support for .m4a
VERBOSE = os.getenv("VERBOSE_ANALYSIS", "0") == "1"

ROOT = os.path.dirname(__file__)
with open(os.path.join(ROOT, "presets.json"), "r", encoding="utf-8") as f:
    PRESETS = json.load(f)

USER_STATE = {}  # user_id -> dict

# -------- UI (Keyboards) --------
def label_format(fmt_key: str) -> str:
    return {"wav16": "WAV 16-bit", "mp3_320": "MP3 320", "wav24": "WAV 24-bit"}[fmt_key]

def kb_main(uid):
    st = USER_STATE.get(uid, PRESETS["defaults"])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎚 Intensity: {st['intensity']}", callback_data="menu_intensity")],
         [InlineKeyboardButton(text=f"🎛 Tone: {st['tone']}", callback_data="menu_tone")],
         [InlineKeyboardButton(text=f"💾 Output: {label_format(st['format'])}", callback_data="menu_format")],
         [InlineKeyboardButton(text=("✅ Auto ON" if st.get("auto") else "🤖 Auto OFF"), callback_data="toggle_auto")]
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
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="WAV 16-bit", callback_data="set_fmt_wav16")],
        [InlineKeyboardButton(text="MP3 320 kbps", callback_data="set_fmt_mp3_320")],
        [InlineKeyboardButton(text="Ultra HD WAV 24-bit", callback_data="set_fmt_wav24")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main"),
         InlineKeyboardButton(text="🏠 Домой", callback_data="go_home")]
    ])

# -------- HANDLERS: MENU AND SETTINGS --------
@dp.message(CommandStart())  # === изменено ===
async def start(m: Message):
    USER_STATE[m.from_user.id] = {
        "intensity": PRESETS["defaults"]["intensity"],
        "tone": PRESETS["defaults"]["tone"],
        "format": PRESETS["defaults"]["format"],
        "auto": True  # Smart Auto is ON by default
    }
    await m.answer(
        "👋 Привет! Я — Mr. Mastering.\n"
        "Пришли аудио-файл **.mp3**, **.m4a** или **.wav** (до ~19 MB), либо **ссылку** на файл в облаке (Google Drive/Dropbox).\n"
        "Форматы мастеринга: WAV 16-bit / MP3 320 / WAV 24-bit.\n"
        "Сейчас включен режим Smart Auto — полный автоматический мастеринг.",
        reply_markup=kb_main(m.from_user.id)
    )

@dp.message(Command("menu"))
async def menu_cmd(m: Message):
    await m.answer("Главное меню:", reply_markup=kb_main(m.from_user.id))

@dp.message(Command("settings"))
async def settings_cmd(m: Message):
    USER_STATE[m.from_user.id] = PRESETS["defaults"].copy() | {"auto": True}
    await m.answer("⚙️ Настройки сброшены. Режим Smart Auto включён.", reply_markup=kb_main(m.from_user.id))

# -------- CALLBACK HANDLERS: INLINE MENU --------
@dp.callback_query()
async def callbacks(c):
    uid = c.from_user.id
    data = c.data
    st = USER_STATE.get(uid, PRESETS["defaults"])
    if data == "go_home":
        # Return to main menu without changing state
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
        # If user adjusts intensity manually, consider that turning Auto off (manual mode)
        # === changed ===
        st["auto"] = False
        await c.message.edit_text(f"Интенсивность: {intensity}", reply_markup=kb_main(uid))
    elif data.startswith("set_tone_"):
        tone = data.split("set_tone_")[1]
        st["tone"] = tone
        # Changing tone manually implies Auto off as well
        # === changed ===
        st["auto"] = False
        await c.message.edit_text(f"Тон: {tone}", reply_markup=kb_main(uid))
    elif data.startswith("set_fmt_"):
        fmt = data.split("set_fmt_")[1]
        st["format"] = fmt
        await c.message.edit_text(f"Формат результата: {label_format(fmt)}", reply_markup=kb_main(uid))
    elif data == "toggle_auto":
        # Toggle between Smart Auto and Manual modes
        st["auto"] = not st.get("auto", True)
        status = "✅ Auto ON" if st["auto"] else "🤖 Auto OFF"
        await c.message.edit_text("Главное меню:", reply_markup=kb_main(uid))
    await c.answer()

# -------- CORE HANDLER: AUDIO FILES --------
@dp.message(F.audio | F.document)
async def on_audio(m: Message):
    file_obj = m.audio or m.document
    if not file_obj:
        return
    name = (file_obj.file_name or "input").lower()
    if not name.endswith(ALLOWED_EXT):
        await m.reply("⚠️ Пожалуйста, пришли аудио-файл с расширением **.mp3**, **.m4a** или **.wav**.", reply_markup=kb_home())
        return

    size = file_obj.file_size or 0
    if _too_big(size, MAX_TG_FILE_MB):
        await m.reply(
            f"⚠️ Файл **{round(size/1024/1024, 1)} MB** слишком большой для загрузки через Telegram.\n"
            f"Отправь **ссылку** на файл (Google Drive или Dropbox), я скачаю и сделаю мастеринг.",
            reply_markup=kb_home()
        )
        return

    # Retrieve user state (intensity, tone, format, auto)
    st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
    inten_key = st["intensity"]
    tone_key = st["tone"]
    fmt_key = st["format"]
    auto_mode = st.get("auto", True)

    await m.reply("🎧 Файл получен. " + ("Анализирую и мастерю…" if auto_mode else "Делаю мастеринг…"), reply_markup=kb_home())
    try:
        with tempfile.TemporaryDirectory() as td:
            in_path = os.path.join(td, name)
            out_path = os.path.join(td, "mastered.wav" if fmt_key.startswith("wav") else "mastered.mp3")

            # Download the file to a temp folder
            file_info = await bot.get_file(file_obj.file_id)
            await bot.download_file(file_info.file_path, in_path)

            # Mastering process
            if auto_mode:
                # Smart Auto Mode: full analysis and automatic parameter selection
                analysis = analyze_file(in_path)  # === changed === use new analysis module
                params = decide_smart_params(analysis)  # get optimal processing params (loudnorm targets, EQ, comp, etc.)
                chain = build_smart_chain(params)      # build FFmpeg audio filter chain from params
                # Verbose logging of analysis
                if VERBOSE:
                    print(f"[Analysis++] {os.path.basename(name)} -> "
                          f"LUFS={analysis['LUFS']:.2f}, LRA={analysis['LRA']:.2f}, TruePeak={analysis['TruePeak_dBFS']:.2f} dBFS, "
                          f"Tilt={analysis['Tilt_dB']:+.2f} dB, SubExcess={analysis['SubExcess']} => {params}", flush=True)
                # Process audio using two-pass loudnorm with the chain
                fmt_args, _ = output_args(fmt_key)
                await ffmpeg_loudnorm_two_pass(in_path, chain, fmt_args, out_path)
                analysis_note = (f" | LUFS={analysis['LUFS']:.1f}, LRA={analysis['LRA']:.1f}, Tilt={analysis['Tilt_dB']:+.1f} dB") if VERBOSE else ""
            else:
                # Manual Mode: still perform analysis for technical adjustments
                analysis = analyze_file(in_path)  # analyze even in manual mode
                if VERBOSE:
                    print(f"[Analysis] {os.path.basename(name)} -> LUFS={analysis['LUFS']:.2f}, TruePeak={analysis['TruePeak_dBFS']:.2f} dBFS, Tilt={analysis['Tilt_dB']:+.2f} dB, SubExcess={analysis['SubExcess']}", flush=True)
                # Build chain from user presets, but incorporate any needed corrections from analysis
                chain = build_ffmpeg_chain(inten_key, tone_key, analysis)
                fmt_args, _ = output_args(fmt_key)
                await ffmpeg_loudnorm_two_pass(in_path, chain, fmt_args, out_path)  # use two-pass even for manual
                analysis_note = ""  # we don't append analysis info in caption for manual (to avoid confusion)
            
            # Check output size and send appropriate format
            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                # If output WAV is too large for Telegram, fallback to MP3 320kbps
                alt_out_path = os.path.join(td, "mastered_320.mp3")
                if auto_mode:
                    await ffmpeg_loudnorm_two_pass(in_path, chain, output_args("mp3_320")[0], alt_out_path)
                else:
                    await ffmpeg_loudnorm_two_pass(in_path, chain, output_args("mp3_320")[0], alt_out_path)
                await m.reply_document(
                    FSInputFile(alt_out_path, filename="mastered_320.mp3"),
                    caption=(f"✅ Готово! Результат: MP3 320 kbps{analysis_note}\n"
                             f"(WAV > {MAX_TG_SEND_MB} MB, поэтому отправлен MP3)"),
                    reply_markup=kb_home()
                )
            else:
                # Send the mastered file in the requested format
                await m.reply_document(
                    FSInputFile(out_path, filename=os.path.basename(out_path)),
                    caption=f"✅ Готово! Результат: {label_format(fmt_key)}{analysis_note}",
                    reply_markup=kb_home()
                )
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}", reply_markup=kb_home())

# -------- CORE HANDLER: LINK (Google Drive/Dropbox) --------
@dp.message(F.text)
async def on_text(m: Message):
    url = (m.text or "").strip()
    if not (is_gdrive(url) or DIRECT_RX.match(url)):
        return  # not a recognized URL, ignore
    await m.reply("⏬ Скачиваю файл по ссылке, выполняю мастеринг…", reply_markup=kb_home())
    try:
        with tempfile.TemporaryDirectory() as td:
            # Determine file extension from URL (default to .wav if unknown)
            ext = ".mp3" if ".mp3" in url.lower() else ".m4a" if ".m4a" in url.lower() else ".wav"
            in_path = os.path.join(td, f"input_from_link{ext}")
            if is_gdrive(url):
                url = gdrive_direct(url) or url
            async with aiohttp.ClientSession() as session:
                await http_download(session, url, in_path, max_mb=256)

            st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
            inten_key = st["intensity"]; tone_key = st["tone"]
            fmt_key = st["format"]; auto_mode = st.get("auto", True)
            out_path = os.path.join(td, "mastered.wav" if fmt_key.startswith("wav") else "mastered.mp3")

            if auto_mode:
                analysis = analyze_file(in_path)
                params = decide_smart_params(analysis)
                chain = build_smart_chain(params)
                if VERBOSE:
                    print(f"[Analysis++] Link -> LUFS={analysis['LUFS']:.2f}, LRA={analysis['LRA']:.2f}, TP={analysis['TruePeak_dBFS']:.2f}, Tilt={analysis['Tilt_dB']:+.2f}, SubExcess={analysis['SubExcess']} => {params}", flush=True)
                fmt_args, _ = output_args(fmt_key)
                await ffmpeg_loudnorm_two_pass(in_path, chain, fmt_args, out_path)
                analysis_note = (f" | LUFS={analysis['LUFS']:.1f}, LRA={analysis['LRA']:.1f}, Tilt={analysis['Tilt_dB']:+.1f} dB") if VERBOSE else ""
            else:
                analysis = analyze_file(in_path)
                if VERBOSE:
                    print(f"[Analysis] Link -> LUFS={analysis['LUFS']:.2f}, TP={analysis['TruePeak_dBFS']:.2f}, Tilt={analysis['Tilt_dB']:+.2f}, SubExcess={analysis['SubExcess']}", flush=True)
                chain = build_ffmpeg_chain(inten_key, tone_key, analysis)
                fmt_args, _ = output_args(fmt_key)
                await ffmpeg_loudnorm_two_pass(in_path, chain, fmt_args, out_path)
                analysis_note = ""

            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                alt_out = os.path.join(td, "mastered_320.mp3")
                await ffmpeg_loudnorm_two_pass(in_path, chain, output_args("mp3_320")[0], alt_out)
                await m.reply_document(
                    FSInputFile(alt_out, filename="mastered_320.mp3"),
                    caption=(f"✅ Готово! Результат: MP3 320 kbps{analysis_note}\n"
                             f"(финальный WAV > {MAX_TG_SEND_MB}MB, поэтому MP3)"),
                    reply_markup=kb_home()
                )
            else:
                await m.reply_document(
                    FSInputFile(out_path, filename=os.path.basename(out_path)),
                    caption=f"✅ Готово! Результат: {label_format(fmt_key)}{analysis_note}",
                    reply_markup=kb_home()
                )
    except Exception as e:
        await m.reply(f"❌ Ошибка при загрузке/мастеринге: {e}", reply_markup=kb_home())

# -------- PRESET-BASED CHAIN (Manual mode) --------
def build_ffmpeg_chain(inten_key: str, tone_key: str, analysis: dict):
    """Construct the FFmpeg filter chain for manual mode using presets, 
    but adapt if needed based on analysis (e.g. add HPF for sub bass)."""
    inten = PRESETS["intensity"][inten_key]
    tone = PRESETS["tone"][tone_key]
    eq_parts = []
    # If analysis indicates excessive sub-bass, add a gentle high-pass filter at 30Hz
    if analysis.get("SubExcess"):
        eq_parts.append("highpass=f=30:width=0.7")  # === changed === auto-correct sub-bass
    # Low shelf from tone preset
    if tone.get("low_shelf"):
        lf = tone["low_shelf"]
        eq_parts.append(f"bass=g={lf['g']}:f={lf['f']}:w={lf['width']}")
    # High shelf from tone preset
    if tone.get("high_shelf"):
        hf = tone["high_shelf"]
        eq_parts.append(f"treble=g={hf['g']}:f={hf['f']}:w={hf['width']}")
    # If no EQ adjustments, use an 'anull' filter
    eq_chain = ",".join(eq_parts) if eq_parts else "anull"
    # Use preset compressor settings
    comp = inten["comp"]
    acompressor = (
        f"acompressor=ratio={comp['ratio']}:threshold={comp['threshold_db']}dB:"
        f"attack={comp['attack']}:release={comp['release']}"
    )
    # Use preset loudnorm targets
    loudnorm = f"loudnorm=I={inten['I']}:TP={inten['TP']}:LRA={inten['LRA']}:print_format=summary"
    # Note: In two-pass mode, print_format=summary will be replaced with json and back.
    chain = f"{eq_chain},{acompressor},{loudnorm}"
    # === changed === 
    # If Smart Auto analysis found track very mono (low stereo width), we do NOT automatically widen in manual mode (to respect user).
    # (We only widen in smart auto mode for now.)
    return chain

def output_args(fmt_key: str):
    # Select FFmpeg output arguments and filename based on desired format
    if fmt_key == "wav16":   return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"
    if fmt_key == "wav24":   return "-ar 48000 -ac 2 -c:a pcm_s24le", "mastered_uhd.wav"
    if fmt_key == "mp3_320": return "-ar 48000 -ac 2 -c:a libmp3lame -b:a 320k", "mastered_320.mp3"
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"

# -------- LOUDNORM UTILS (Two-Pass Implementation) --------
def _too_big(bytes_size: int, mb: int) -> bool:
    return bytes_size > mb * 1024 * 1024

# (Regex patterns for Google Drive etc. are unchanged)
GDRIVE_RX = re.compile(r"(?:https?://)?(?:drive\.google\.com)/(?:file/d/|open\?id=|uc\?id=)([\w-]+)")
DIRECT_RX = re.compile(r"^https?://.*\.(mp3|wav|m4a)(\?.*)?$", re.IGNORECASE)
def is_gdrive(url: str) -> bool: return GDRIVE_RX.search(url) is not None
def gdrive_direct(url: str) -> Optional[str]:
    m = GDRIVE_RX.search(url)
    if not m: return None
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

# (Two-pass loudnorm helper functions and ffmpeg_loudnorm_two_pass remain mostly unchanged)
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
            if depth == 0: start = i
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
    # Ensure TP target is within [-9, 0] dBFS to avoid invalid values
    TP = float(np.clip(TP, -9.0, 0.0))
    return I, TP, LRA

async def ffmpeg_loudnorm_two_pass(in_path: str, af_chain: str, out_args: str, out_path: str):
    """Run loudnorm in two-pass mode for given audio filter chain (which must end with loudnorm)."""
    if "loudnorm" not in af_chain:
        # If loudnorm filter is not in chain, we can just do a single pass (no normalization needed)
        cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{af_chain}" {out_args} {shlex.quote(out_path)}'
        proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, err = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError("ffmpeg processing failed: " + err.decode("utf-8", errors="ignore"))
        return

    # PASS 1 – force JSON output to measure input loudness parameters
    pass1_chain = _force_print_format_json(af_chain)
    pass1_cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{pass1_chain}" -f null -'
    p1 = await asyncio.create_subprocess_shell(pass1_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err1 = await p1.communicate()
    if p1.returncode != 0:
        raise RuntimeError("ffmpeg pass1 failed: " + err1.decode("utf-8", errors="ignore"))
    text = err1.decode("utf-8", "ignore")
    stats = _extract_last_json_block(text)
    # Extract target values from original chain
    target_I, target_TP, target_LRA = _extract_loudnorm_targets(af_chain)
    # Build measured parameters string for pass2 (if stats available)
    if stats:
        measured_args = (
            f"I={target_I}:TP={target_TP}:LRA={target_LRA}:"
            f"measured_I={stats.get('input_i', '-14')}:"
            f"measured_LRA={stats.get('input_lra', '7')}:"
            f"measured_TP={stats.get('input_tp', '-2')}:"
            f"measured_thresh={stats.get('input_thresh', '-24')}:"
            f"offset={stats.get('target_offset', '0')}:print_format=summary"
        )
        # Replace the loudnorm filter args with measured args for pass2
        prefix, _ = af_chain.rsplit("loudnorm=", 1)
        af_chain = prefix + "loudnorm=" + measured_args
    # PASS 2 – apply loudness normalization with measured values
    pass2_cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{af_chain}" {out_args} {shlex.quote(out_path)}'
    p2 = await asyncio.create_subprocess_shell(pass2_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err2 = await p2.communicate()
    if p2.returncode != 0:
        raise RuntimeError("ffmpeg pass2 failed: " + err2.decode("utf-8", errors="ignore"))
# -------- MAIN --------
# === добавлено ===
async def _runner():
    # Сбрасываем старый webhook (если бот был подключён через вебхуки)
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
