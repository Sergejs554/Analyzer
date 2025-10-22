#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, sys, shlex, json, asyncio, tempfile, math
from typing import Optional
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
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
    print("[FATAL] Invalid BOT_TOKEN. Fix env var BOT_TOKEN.", flush=True); sys.exit(1)

bot = Bot(token)
dp = Dispatcher()

# -------- SETTINGS --------
MAX_TG_FILE_MB = int(os.getenv("MAX_TG_FILE_MB", "19"))        # лимит на скачивание из TG
MAX_TG_SEND_MB = int(os.getenv("MAX_TG_SEND_MB", "49"))        # лимит на отправку документа в TG
ALLOWED_EXT = (".mp3", ".wav")

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

def kb_intensity():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Low", callback_data="set_intensity_low"),
         InlineKeyboardButton(text="Balanced", callback_data="set_intensity_balanced"),
         InlineKeyboardButton(text="High", callback_data="set_intensity_high")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main")]
    ])

def kb_tone():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Warm", callback_data="set_tone_warm"),
         InlineKeyboardButton(text="Balanced", callback_data="set_tone_balanced"),
         InlineKeyboardButton(text="Bright", callback_data="set_tone_bright")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main")]
    ])

def kb_format():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="WAV 16-bit", callback_data="set_fmt_wav16")],
        [InlineKeyboardButton(text="MP3 320 kbps", callback_data="set_fmt_mp3_320")],
        [InlineKeyboardButton(text="Ultra HD WAV 24-bit", callback_data="set_fmt_wav24")],
        [InlineKeyboardButton(text="← Back", callback_data="back_main")]
    ])

# -------- ANALYZE --------
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

# -------- FFMPEG CHAIN --------
def build_ffmpeg_chain(inten_key: str, tone_key: str):
    """
    EQ полками (bass/treble) -> компрессор без makeup -> loudnorm.
    Attack/release в мс, threshold в dB (с 'dB'), ratio > 1.
    """
    inten = PRESETS["intensity"][inten_key]
    tone  = PRESETS["tone"][tone_key]

    eq_parts = []
    if tone.get("low_shelf"):
        lf = tone["low_shelf"]   # {f, width, g}
        eq_parts.append(f"bass=g={lf['g']}:f={lf['f']}:w={lf['width']}")
    if tone.get("high_shelf"):
        hf = tone["high_shelf"]  # {f, width, g}
        eq_parts.append(f"treble=g={hf['g']}:f={hf['f']}:w={hf['width']}")

    eq_chain = ",".join(eq_parts) if eq_parts else "anull"

    # ВНИМАНИЕ: без makeup (или makeup=1). Это фикс ошибки.
    comp = inten["comp"]  # {ratio, threshold_db, attack, release}
    acompressor = (
        f"acompressor="
        f"ratio={comp['ratio']}:"
        f"threshold={comp['threshold_db']}dB:"
        f"attack={comp['attack']}:"
        f"release={comp['release']}"
    )

    loudnorm = (
        f"loudnorm=I={inten['I']}:TP={inten['TP']}:LRA={inten['LRA']}:print_format=summary"
    )

    return f"{eq_chain},{acompressor},{loudnorm}"
def output_args(fmt_key:str):
    if fmt_key=="wav16":   return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"
    if fmt_key=="wav24":   return "-ar 48000 -ac 2 -c:a pcm_s24le", "mastered_uhd.wav"
    if fmt_key=="mp3_320": return "-ar 48000 -ac 2 -codec:a libmp3lame -b:a 320k", "mastered_320.mp3"
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"

async def process_audio(in_path: str, out_path: str, intensity: str, tone: str, fmt_key: str):
    # проверим ffmpeg
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
    # прямой линк с принудительным скачиванием
    return f"https://drive.google.com/uc?export=download&id={file_id}"

async def http_download(session:aiohttp.ClientSession, url:str, dst_path:str, max_mb:int=256)->int:
    """Скачивает по HTTP в файл. Возвращает размер в байтах. Ограничим до max_mb."""
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

# -------- HANDLERS --------
@dp.message(Command("start"))
async def start(m: Message):
    USER_STATE[m.from_user.id] = {"intensity": PRESETS["defaults"]["intensity"],
                                  "tone": PRESETS["defaults"]["tone"],
                                  "format": PRESETS["defaults"]["format"],
                                  "auto": True}
    await m.answer(
        "Йо! Я — Mr Mastering.\n"
        "Пришли трек **.mp3** или **.wav** (до ~19 MB в Telegram), либо **ссылку** на Google Drive/Dropbox.\n"
        "Форматы вывода: WAV16 / MP3 320 / WAV24.\n"
        "Auto — включён.", reply_markup=kb_main(m.from_user.id)
    )

@dp.callback_query(F.data == "menu_intensity")
async def menu_intensity(c): await c.message.edit_text("Выбери Intensity:", reply_markup=kb_intensity()); await c.answer()

@dp.callback_query(F.data == "menu_tone")
async def menu_tone(c): await c.message.edit_text("Выбери Tone:", reply_markup=kb_tone()); await c.answer()

@dp.callback_query(F.data == "menu_format")
async def menu_format(c): await c.message.edit_text("Выбери формат вывода:", reply_markup=kb_format()); await c.answer()

@dp.callback_query(F.data == "back_main")
async def back_main(c): await c.message.edit_text("Главное меню:", reply_markup=kb_main(c.from_user.id)); await c.answer()

@dp.callback_query(F.data.startswith("set_intensity_"))
async def set_intensity(c):
    val = c.data.replace("set_intensity_", "")
    USER_STATE[c.from_user.id]["intensity"] = val
    await c.message.edit_text(f"Intensity = {val}\nКинь аудио или настрой Tone/Format.", reply_markup=kb_main(c.from_user.id)); await c.answer()

@dp.callback_query(F.data.startswith("set_tone_"))
async def set_tone(c):
    val = c.data.replace("set_tone_", "")
    USER_STATE[c.from_user.id]["tone"] = val
    await c.message.edit_text(f"Tone = {val}\nКинь аудио или настрой Intensity/Format.", reply_markup=kb_main(c.from_user.id)); await c.answer()

@dp.callback_query(F.data.startswith("set_fmt_"))
async def set_fmt(c):
    key = c.data.replace("set_fmt_", "")
    mapping = {"wav16":"wav16","mp3_320":"mp3_320","wav24":"wav24"}
    key = mapping.get(key, "wav16")
    USER_STATE[c.from_user.id]["format"] = key
    await c.message.edit_text(f"Output = {label_format(key)}\nКинь аудио.", reply_markup=kb_main(c.from_user.id)); await c.answer()

@dp.callback_query(F.data == "toggle_auto")
async def toggle_auto(c):
    st = USER_STATE.get(c.from_user.id, PRESETS["defaults"])
    st["auto"] = not st.get("auto", False); USER_STATE[c.from_user.id] = st
    await c.message.edit_text(("🤖 Auto включён. Пришли аудио — выберу Intensity/Tone сам."
                               if st["auto"] else "🤖 Auto выключен. Выбери пресеты и пришли аудио."),
                              reply_markup=kb_main(c.from_user.id)); await c.answer()

def _too_big(bytes_size:int, mb:int)->bool:
    return bytes_size > mb*1024*1024

@dp.message(F.audio | F.document)
async def on_audio(m: Message):
    file = m.audio or m.document
    if not file: return
    name = (file.file_name or "input").lower()
    if not name.endswith(ALLOWED_EXT):
        await m.reply("Пришли файл с расширением **.mp3** или **.wav** 🙏"); return

    # защита от лимита Telegram на скачивание
    size = file.file_size or 0
    if _too_big(size, MAX_TG_FILE_MB):
        await m.reply(
            f"⚠️ Файл **{round(size/1024/1024,1)} MB** слишком большой для Telegram-скачивания.\n"
            f"Кинь **ссылку** на Google Drive/Dropbox/WeTransfer — я скачаю и сделаю мастеринг."
        )
        return

    st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
    inten, tone, fmtk, auto = st["intensity"], st["tone"], st["format"], st.get("auto", True)

    await m.reply("Принял файл. " + ("Анализирую и мастерю…" if auto else "Делаю мастеринг…"))
    try:
        with tempfile.TemporaryDirectory() as td:
            in_path = os.path.join(td, name)
            out_path = os.path.join(td, ("mastered.wav" if fmtk.startswith("wav") else "mastered.mp3"))
            fobj = await bot.get_file(file.file_id)
            await bot.download_file(fobj.file_path, in_path)

            if auto:
                I, tilt = analyze_lufs_and_tilt(in_path)
                inten, tone = choose_presets_auto(I, tilt)

            await process_audio(in_path, out_path, inten, tone, fmtk)

            # если итоговый файл слишком большой для отправки — фоллбэк в mp3
            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                mp3_path = os.path.join(td, "mastered_320.mp3")
                _, _name = output_args("mp3_320")
                await process_audio(in_path, mp3_path, inten, tone, "mp3_320")
                await m.reply_document(open(mp3_path, "rb"),
                    caption=f"Готово ✅  Intensity={inten}, Tone={tone}, Format=MP3 320\n"
                            f"(WAV >{MAX_TG_SEND_MB}MB — отправил MP3 фоллбэк)")
                return

            await m.reply_document(open(out_path, "rb"),
                                   caption=f"Готово ✅  Intensity={inten}, Tone={tone}, Format={label_format(fmtk)}")
    except Exception as e:
        await m.reply(f"Ошибка: {e}")

@dp.message(F.text)
async def on_text(m: Message):
    """Приём ссылок (Google Drive, прямые .mp3/.wav)"""
    url = m.text.strip()
    if not (is_gdrive(url) or DIRECT_RX.match(url)):
        return  # игнорим обычный текст

    await m.reply("Окей, скачиваю по ссылке и делаю мастеринг…")
    try:
        with tempfile.TemporaryDirectory() as td, aiohttp.ClientSession() as session:
            # определяем имя
            in_path = os.path.join(td, "input_from_link")
            if is_gdrive(url):
                url = gdrive_direct(url) or url
            # добавим расширение по типу ссылки (по крайней мере mp3/wav)
            ext = ".mp3" if ".mp3" in url.lower() else ".wav"
            in_path += ext

            # качаем (ограничим до 256MB на всякий)
            await http_download(session, url, in_path, max_mb=256)

            st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
            inten, tone, fmtk, auto = st["intensity"], st["tone"], st["format"], st.get("auto", True)

            if auto:
                I, tilt = analyze_lufs_and_tilt(in_path)
                inten, tone = choose_presets_auto(I, tilt)

            out_path = os.path.join(td, ("mastered.wav" if fmtk.startswith("wav") else "mastered.mp3"))
            await process_audio(in_path, out_path, inten, tone, fmtk)

            out_size = os.path.getsize(out_path)
            if _too_big(out_size, MAX_TG_SEND_MB):
                mp3_path = os.path.join(td, "mastered_320.mp3")
                await process_audio(in_path, mp3_path, inten, tone, "mp3_320")
                await m.reply_document(open(mp3_path, "rb"),
                    caption=f"Готово ✅  Intensity={inten}, Tone={tone}, Format=MP3 320\n"
                            f"(WAV >{MAX_TG_SEND_MB}MB — отправил MP3 фоллбэк)")
                return

            await m.reply_document(open(out_path, "rb"),
                                   caption=f"Готово ✅  Intensity={inten}, Tone={tone}, Format={label_format(fmtk)}")
    except Exception as e:
        await m.reply(f"Ошибка при обработке ссылки: {e}")

# -------- MAIN --------
def main():
    print("Mr Mastering bot is running…")
    asyncio.run(dp.start_polling(bot))

if __name__ == "__main__":
    main()
