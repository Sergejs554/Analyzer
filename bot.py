#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, sys, shlex, json, asyncio, tempfile
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, BotCommand, BotCommandScopeDefault, MenuButtonCommands
)
from aiogram.filters import Command
import aiohttp

import numpy as np
import librosa, pyloudnorm as pyln

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
ALLOWED_EXT = (".mp3", ".wav")
VERBOSE = os.getenv("VERBOSE_ANALYSIS", "0") == "1"

ROOT = os.path.dirname(__file__)
with open(os.path.join(ROOT, "presets.json"), "r", encoding="utf-8") as f:
    PRESETS = json.load(f)

USER_STATE = {}  # user_id -> dict

# -------- UI --------
def label_format(fmt_key:str)->str:
    return {"wav16":"WAV 16-bit","mp3_320":"MP3 320","wav24":"WAV 24-bit"}[fmt_key]

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

# -------- SIMPLE AUTO (оставляем на случай Auto OFF) --------
def analyze_lufs_and_tilt(path:str, sr_target=48000):
    y, sr = librosa.load(path, sr=sr_target, mono=True)
    y, _ = librosa.effects.trim(y, top_db=40)
    meter = pyln.Meter(sr)
    I = float(meter.integrated_loudness(y))
    S = np.abs(librosa.stft(y, n_fft=8192, hop_length=2048, window="hann"))**2
    freqs = librosa.fft_frequencies(sr=sr, n_fft=8192)
    psd = np.mean(S, axis=1) + 1e-18
    def band(lo, hi):
        idx = np.where((freqs>=lo)&(freqs<hi))[0]
        return float(10*np.log10(np.mean(psd[idx]))) if idx.size>0 else 0.0
    hi = band(8000, 12000); lo = band(150, 300)
    return I, (hi-lo)

def choose_presets_auto(I:float, tilt:float):
    tone = "balanced"
    if tilt <= -0.8: tone="bright"
    elif tilt >= 0.8: tone="warm"
    intensity = "balanced" if I <= -14.5 else "low"
    return intensity, tone

# -------- AUTO-PRO (под конкретный трек) --------
def analyze_track_pro(path: str, sr_target=48000):
    y, sr = librosa.load(path, sr=sr_target, mono=True)
    y, _ = librosa.effects.trim(y, top_db=40)
    if len(y) == 0:
        raise RuntimeError("Empty audio after trim")

    peak = float(np.max(np.abs(y)))
    rms = float(np.sqrt(np.mean(y**2)))
    rms_db = 20*np.log10(rms + 1e-12)
    tp_dbfs = 20*np.log10(peak + 1e-12)

    meter = pyln.Meter(sr)
    I = float(meter.integrated_loudness(y))

    S = np.abs(librosa.stft(y, n_fft=8192, hop_length=2048, window="hann"))**2
    freqs = librosa.fft_frequencies(sr=sr, n_fft=8192)
    psd = np.mean(S, axis=1) + 1e-18
    def band_db(lo, hi):
        idx = np.where((freqs>=lo)&(freqs<hi))[0]
        return float(10*np.log10(np.mean(psd[idx]))) if idx.size>0 else 0.0
    lo_db = band_db(150, 300)
    hi_db = band_db(8000, 12000)
    tilt = hi_db - lo_db

    sub_db = band_db(20, 40)
    bass_db = band_db(60, 120)
    sub_excess = (sub_db - bass_db) > 2.0

    return {"sr": sr, "I": I, "rms_db": rms_db, "tp_dbfs": tp_dbfs, "tilt": tilt, "sub_excess": bool(sub_excess)}

def decide_params_from_analysis(A: dict):
    I = A["I"]; rms_db = A["rms_db"]; tilt = float(np.clip(A["tilt"], -4.0, 4.0)); sub_excess = A["sub_excess"]

    target_I = float(np.clip(np.interp(I, [-22, -16, -12, -10], [-16, -14.5, -13, -12]), -16.5, -12.0))
    target_TP = -1.2
    target_LRA = 6.0

    high_g = float(np.interp(tilt, [-4, 0, 4], [ +2.5, 0.0, -2.5 ]))
    low_g  = float(np.interp(tilt, [-4, 0, 4], [ -1.5, 0.0, +1.0 ]))
    hpf = bool(sub_excess)

    if rms_db < -24:
        ratio = 1.4; thr = rms_db + 6
    elif rms_db < -20:
        ratio = 1.6; thr = rms_db + 4
    elif rms_db < -16:
        ratio = 1.8; thr = rms_db + 2
    else:
        ratio = 2.2; thr = rms_db + 1

    comp = {"ratio": round(ratio, 2), "threshold_db": round(thr, 1), "attack": 20, "release": 180}
    tone = {
        "low_shelf":  {"g": round(low_g, 2),  "f": 250,  "width": 1.0},
        "high_shelf": {"g": round(high_g, 2), "f": 6500, "width": 0.8},
        "hpf": hpf
    }
    return {"loudnorm": {"I": round(target_I,2), "TP": target_TP, "LRA": target_LRA}, "tone": tone, "comp": comp}

def build_chain_from_params(P: dict):
    tone, comp, ln = P["tone"], P["comp"], P["loudnorm"]
    eq_parts = []
    if tone.get("hpf"):
        eq_parts.append("highpass=f=30:width=0.7")
    if tone.get("low_shelf"):
        lf = tone["low_shelf"]
        eq_parts.append(f"bass=g={lf['g']}:f={lf['f']}:w={lf['width']}")
    if tone.get("high_shelf"):
        hf = tone["high_shelf"]
        eq_parts.append(f"treble=g={hf['g']}:f={hf['f']}:w={hf['width']}")
    eq_chain = ",".join(eq_parts) if eq_parts else "anull"

    acompressor = (
        f"acompressor=ratio={comp['ratio']}:"
        f"threshold={comp['threshold_db']}dB:"
        f"attack={comp['attack']}:release={comp['release']}"
    )
    loudnorm = f"loudnorm=I={ln['I']}:TP={ln['TP']}:LRA={ln['LRA']}:print_format=summary"
    return f"{eq_chain},{acompressor},{loudnorm}"

# --- helpers to force print_format=json and parse last JSON safely ---
def _force_print_format_json(chain: str) -> str:
    # заменяем только последний loudnorm=...print_format=*
    if "loudnorm=" not in chain:
        return chain
    # если явно указан print_format, меняем на json
    if "print_format=" in chain:
        return re.sub(r"(loudnorm=[^,]*print_format=)(\w+)",
                      r"\1json", chain, count=1)
    # если нет, просто добавим
    return re.sub(r"(loudnorm=[^,]*)$", r"\1:print_format=json", chain, count=1)

def _extract_last_json_block(text: str) -> Optional[dict]:
    # пробегаемся по тексту и достаём последний валидный JSON по балансировке скобок
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

async def ffmpeg_loudnorm_two_pass(in_path: str, af_chain_with_loudnorm: str, out_args: str, out_path: str):
    if "loudnorm" not in af_chain_with_loudnorm:
        raise RuntimeError("Chain must end with loudnorm")

    # PASS 1: принуждаем print_format=json, чтобы получить корректный JSON
    pass1_chain = _force_print_format_json(af_chain_with_loudnorm)
    pass1_cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{pass1_chain}" -f null -'
    p1 = await asyncio.create_subprocess_shell(pass1_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err1 = await p1.communicate()
    if p1.returncode != 0:
        raise RuntimeError("ffmpeg pass1 failed: " + err1.decode("utf-8", errors="ignore"))

    text = err1.decode("utf-8", "ignore")
    js = _extract_last_json_block(text)

    # PASS 2: если JSON нашли — подставляем measured_*; иначе — оставляем как есть
    pass2_ln = af_chain_with_loudnorm
    if js:
        measured = (
            f"loudnorm="
            f"I={js.get('target_i', js.get('input_i', '-14'))}:"
            f"TP={js.get('target_tp', js.get('input_tp', '-1.2'))}:"
            f"LRA={js.get('target_lra', js.get('input_lra', '7'))}:"
            f"measured_I={js.get('input_i','-14')}:"
            f"measured_LRA={js.get('input_lra','7')}:"
            f"measured_TP={js.get('input_tp','-1.2')}:"
            f"measured_thresh={js.get('input_thresh','-26')}:"
            f"offset={js.get('target_offset','0')}:print_format=summary"
        )
        pass2_ln = re.sub(r"loudnorm=[^,]*print_format=\w+", measured, af_chain_with_loudnorm)

    pass2_cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{pass2_ln}" {out_args} {shlex.quote(out_path)}'
    p2 = await asyncio.create_subprocess_shell(pass2_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err2 = await p2.communicate()
    if p2.returncode != 0:
        raise RuntimeError("ffmpeg pass2 failed: " + err2.decode("utf-8", errors="ignore"))

# -------- ПРЕСЕТНЫЙ FFMPEG (когда Auto выключен) --------
def build_ffmpeg_chain(inten_key: str, tone_key: str):
    inten = PRESETS["intensity"][inten_key]
    tone  = PRESETS["tone"][tone_key]

    eq_parts = []
    if tone.get("low_shelf"):
        lf = tone["low_shelf"]
        eq_parts.append(f"bass=g={lf['g']}:f={lf['f']}:w={lf['width']}")
    if tone.get("high_shelf"):
        hf = tone["high_shelf"]
        eq_parts.append(f"treble=g={hf['g']}:f={hf['f']}:w={hf['width']}")
    eq_chain = ",".join(eq_parts) if eq_parts else "anull"

    comp = inten["comp"]  # {ratio, threshold_db, attack, release}
    acompressor = (
        f"acompressor=ratio={comp['ratio']}:"
        f"threshold={comp['threshold_db']}dB:"
        f"attack={comp['attack']}:"
        f"release={comp['release']}"
    )
    loudnorm = f"loudnorm=I={inten['I']}:TP={inten['TP']}:LRA={inten['LRA']}:print_format=summary"
    return f"{eq_chain},{acompressor},{loudnorm}"

def output_args(fmt_key:str):
    if fmt_key=="wav16":   return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"
    if fmt_key=="wav24":   return "-ar 48000 -ac 2 -c:a pcm_s24le", "mastered_uhd.wav"
    if fmt_key=="mp3_320": return "-ar 48000 -ac 2 -codec:a libmp3lame -b:a 320k", "mastered_320.mp3"
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"

async def process_audio(in_path: str, out_path: str, intensity: str, tone: str, fmt_key: str):
    from shutil import which
    if which("ffmpeg") is None:
        raise RuntimeError("ffmpeg not found. Add it in nixpacks.toml (nixPkgs=['ffmpeg']).")
    af = build_ffmpeg_chain(intensity, tone)
    fmt_args, _ = output_args(fmt_key)
    cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{af}" {fmt_args} {shlex.quote(out_path)}'
    proc = await asyncio.create_subprocess_shell(cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg failed: " + err.decode("utf-8", errors="ignore"))

# -------- UTILS: LINKS & DOWNLOAD --------
GDRIVE_RX = re.compile(r"(?:https?://)?(?:drive\.google\.com)/(?:file/d/|open\?id=|uc\?id=)([\w-]+)")
DIRECT_RX = re.compile(r"^https?://.*\.(mp3|wav)(\?.*)?$", re.IGNORECASE)

def is_gdrive(url:str)->bool: return GDRIVE_RX.search(url) is not None

def gdrive_direct(url:str)->Optional[str]:
    m = GDRIVE_RX.search(url)
    if not m: return None
    file_id = m.group(1)
    return f"https://drive.google.com/uc?export=download&id={file_id}"

async def http_download(session:aiohttp.ClientSession, url:str, dst_path:str, max_mb:int=256)->int:
    total = 0
    async with session.get(url, timeout=120) as r:
        r.raise_for_status()
        with open(dst_path, "wb") as f:
            async for chunk in r.content.iter_chunked(1<<14):
                if not chunk: break
                total += len(chunk)
                if total > max_mb*1024*1024:
                    raise RuntimeError("Remote file too big")
                f.write(chunk)
    return total

def _too_big(bytes_size:int, mb:int)->bool:
    return bytes_size > mb*1024*1024

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

# -------- HANDLERS --------
@dp.message(Command("start"))
async def start(m: Message):
    USER_STATE[m.from_user.id] = {
        "intensity": PRESETS["defaults"]["intensity"],
        "tone": PRESETS["defaults"]["tone"],
        "format": PRESETS["defaults"]["format"],
        "auto": True
    }
    await m.answer(
        "Йо! Я — Mr Mastering.\n"
        "Пришли трек **.mp3** или **.wav** (до ~19 MB в Telegram), либо **ссылку** на Google Drive/Dropbox.\n"
        "Форматы вывода: WAV16 / MP3 320 / WAV24.\n"
        "Auto — включён.",
        reply_markup=kb_main(m.from_user.id)
    )

@dp.message(Command("menu"))
async def menu_cmd(m: Message):
    await m.answer("Главное меню:", reply_markup=kb_main(m.from_user.id))

@dp.message(Command("settings"))
async def settings_cmd(m: Message):
    USER_STATE[m.from_user.id] = PRESETS["defaults"].copy() | {"auto": True}
    await m.answer("Настройки сброшены. Главное меню:", reply_markup=kb_main(m.from_user.id))

@dp.callback_query(F.data == "go_home")
async def go_home(c):
    await c.message.answer("Главное меню:", reply_markup=kb_main(c.from_user.id))
    await c.answer()

@dp.callback_query(F.data == "menu_intensity")
async def menu_intensity(c):
    await c.message.edit_text("Выбери Intensity:", reply_markup=kb_intensity()); await c.answer()

@dp.callback_query(F.data == "menu_tone")
async def menu_tone(c):
    await c.message.edit_text("Выбери Tone:", reply_markup=kb_tone()); await c.answer()

@dp.callback_query(F.data == "menu_format")
async def menu_format(c):
    await c.message.edit_text("Выбери формат вывода:", reply_markup=kb_format()); await c.answer()

@dp.callback_query(F.data == "back_main")
async def back_main(c):
    await c.message.edit_text("Главное меню:", reply_markup=kb_main(c.from_user.id)); await c.answer()

@dp.callback_query(F.data.startswith("set_intensity_"))
async def set_intensity(c):
    val = c.data.replace("set_intensity_", "")
    USER_STATE[c.from_user.id]["intensity"] = val
    await c.message.edit_text(
        f"Intensity = {val}\nКинь аудио или настрой Tone/Format.",
        reply_markup=kb_main(c.from_user.id)
    ); await c.answer()

@dp.callback_query(F.data.startswith("set_tone_"))
async def set_tone(c):
    val = c.data.replace("set_tone_", "")
    USER_STATE[c.from_user.id]["tone"] = val
    await c.message.edit_text(
        f"Tone = {val}\nКинь аудио или настрой Intensity/Format.",
        reply_markup=kb_main(c.from_user.id)
    ); await c.answer()

@dp.callback_query(F.data.startswith("set_fmt_"))
async def set_fmt(c):
    key = c.data.replace("set_fmt_", "")
    mapping = {"wav16":"wav16","mp3_320":"mp3_320","wav24":"wav24"}
    key = mapping.get(key, "wav16")
    USER_STATE[c.from_user.id]["format"] = key
    await c.message.edit_text(
        f"Output = {label_format(key)}\nКинь аудио.",
        reply_markup=kb_main(c.from_user.id)
    ); await c.answer()

@dp.callback_query(F.data == "toggle_auto")
async def toggle_auto(c):
    st = USER_STATE.get(c.from_user.id, PRESETS["defaults"])
    st["auto"] = not st.get("auto", False)
    USER_STATE[c.from_user.id] = st
    await c.message.edit_text(
        ("🤖 Auto включён. Пришли аудио — выберу Intensity/Tone сам."
         if st["auto"] else
         "🤖 Auto выключен. Выбери пресеты и пришли аудио."),
        reply_markup=kb_main(c.from_user.id)
    ); await c.answer()

# ---- CORE HANDLER: FILE ----
@dp.message(F.audio | F.document)
async def on_audio(m: Message):
    file = m.audio or m.document
    if not file: return
    name = (file.file_name or "input").lower()
    if not name.endswith(ALLOWED_EXT):
        await m.reply("Пришли файл с расширением **.mp3** или **.wav** 🙏", reply_markup=kb_home())
        return

    size = file.file_size or 0
    if _too_big(size, MAX_TG_FILE_MB):
        await m.reply(
            f"⚠️ Файл **{round(size/1024/1024,1)} MB** слишком большой для Telegram-скачивания.\n"
            f"Кинь **ссылку** на Google Drive/Dropbox — я скачаю и сделаю мастеринг.",
            reply_markup=kb_home()
        )
        return

    st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
    inten, tone, fmtk, auto = st["intensity"], st["tone"], st["format"], st.get("auto", True)

    await m.reply("Принял файл. " + ("Анализирую и мастерю…" if auto else "Делаю мастеринг…"), reply_markup=kb_home())
    try:
        with tempfile.TemporaryDirectory() as td:
            in_path = os.path.join(td, name)
            out_path = os.path.join(td, ("mastered.wav" if fmtk.startswith("wav") else "mastered.mp3"))

            fobj = await bot.get_file(file.file_id)
            await bot.download_file(fobj.file_path, in_path)

            if auto:
                # --- AUTO-PRO ---
                A = analyze_track_pro(in_path)
                P = decide_params_from_analysis(A)
                chain = build_chain_from_params(P)
                if VERBOSE:
                    print(f"[ANALYZE] LUFS={A['I']:.1f} | RMS={A['rms_db']:.1f} dBFS | TP={A['tp_dbfs']:.1f} dBFS | tilt={A['tilt']:+.2f} dB | sub_excess={A['sub_excess']} -> {P}", flush=True)
                fmt_args, _ = output_args(fmtk)
                await ffmpeg_loudnorm_two_pass(in_path, chain, fmt_args, out_path)
                analysis_note = (f" | LUFS={A['I']:.1f} | tilt={A['tilt']:+.2f} dB") if VERBOSE else ""
            else:
                # --- Ручные пресеты ---
                await process_audio(in_path, out_path, inten, tone, fmtk)
                analysis_note = ""

            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                mp3_path = os.path.join(td, "mastered_320.mp3")
                if auto:
                    await ffmpeg_loudnorm_two_pass(in_path, chain, output_args("mp3_320")[0], mp3_path)
                else:
                    await process_audio(in_path, mp3_path, inten, tone, "mp3_320")
                await m.reply_document(
                    FSInputFile(mp3_path, filename="mastered_320.mp3"),
                    caption=(f"Готово ✅  Format=MP3 320{analysis_note}\n"
                             f"(WAV >{MAX_TG_SEND_MB}MB — отправил MP3)"),
                    reply_markup=kb_home()
                )
                return

            await m.reply_document(
                FSInputFile(out_path, filename=os.path.basename(out_path)),
                caption=f"Готово ✅  Format={label_format(fmtk)}{analysis_note}",
                reply_markup=kb_home()
            )
    except Exception as e:
        await m.reply(f"Ошибка: {e}", reply_markup=kb_home())

# ---- CORE HANDLER: LINKS ----
@dp.message(F.text)
async def on_text(m: Message):
    url = (m.text or "").strip()
    if not (is_gdrive(url) or DIRECT_RX.match(url)):
        return
    await m.reply("Окей, скачиваю по ссылке и делаю мастеринг…", reply_markup=kb_home())
    try:
        with tempfile.TemporaryDirectory() as td:
            ext = ".mp3" if ".mp3" in url.lower() else ".wav"
            in_path = os.path.join(td, f"input_from_link{ext}")
            if is_gdrive(url):
                url = gdrive_direct(url) or url
            async with aiohttp.ClientSession() as session:
                await http_download(session, url, in_path, max_mb=256)

            st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
            inten, tone, fmtk, auto = st["intensity"], st["tone"], st["format"], st.get("auto", True)

            out_path = os.path.join(td, ("mastered.wav" if fmtk.startswith("wav") else "mastered.mp3"))
            if auto:
                A = analyze_track_pro(in_path)
                P = decide_params_from_analysis(A)
                chain = build_chain_from_params(P)
                if VERBOSE:
                    print(f"[ANALYZE] LUFS={A['I']:.1f} | RMS={A['rms_db']:.1f} dBFS | TP={A['tp_dbfs']:.1f} dBFS | tilt={A['tilt']:+.2f} dB | sub_excess={A['sub_excess']} -> {P}", flush=True)
                fmt_args, _ = output_args(fmtk)
                await ffmpeg_loudnorm_two_pass(in_path, chain, fmt_args, out_path)
                analysis_note = (f" | LUFS={A['I']:.1f} | tilt={A['tilt']:+.2f} dB") if VERBOSE else ""
            else:
                await process_audio(in_path, out_path, inten, tone, fmtk)
                analysis_note = ""

            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                mp3_path = os.path.join(td, "mastered_320.mp3")
                if auto:
                    await ffmpeg_loudnorm_two_pass(in_path, chain, output_args("mp3_320")[0], mp3_path)
                else:
                    await process_audio(in_path, mp3_path, inten, tone, "mp3_320")
                await m.reply_document(
                    FSInputFile(mp3_path, filename="mastered_320.mp3"),
                    caption=(f"Готово ✅  Format=MP3 320{analysis_note}\n"
                             f"(WAV >{MAX_TG_SEND_MB}MB — отправил MP3)"),
                    reply_markup=kb_home()
                )
                return

            await m.reply_document(
                FSInputFile(out_path, filename=os.path.basename(out_path)),
                caption=f"Готово ✅  Format={label_format(fmtk)}{analysis_note}",
                reply_markup=kb_home()
            )
    except Exception as e:
        await m.reply(f"Ошибка при обработке ссылки: {e}", reply_markup=kb_home())

# -------- MAIN --------
async def _runner():
    await setup_menu()
    print("Mr Mastering bot is running…")
    await dp.start_polling(bot)

def main():
    asyncio.run(_runner())

if __name__ == "__main__":
    main()
