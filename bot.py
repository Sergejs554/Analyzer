#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, asyncio, json, tempfile, subprocess, shlex
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

import numpy as np
import librosa, pyloudnorm as pyln
import os, re, sys

# 1) читаем из окружения только BOT_TOKEN
raw_token = os.getenv("BOT_TOKEN") or ""
# 2) убираем невидимые символы (zero-width, NBSP, BOM) и пробелы по краям
clean_token = (raw_token
               .strip()
               .replace("\ufeff", "")   # BOM
               .replace("\u200b", "")   # zero width space
               .replace("\u2060", "")   # word joiner
               .replace("\xa0", ""))    # NBSP

# 3) быстрый дебаг в логи (покажем длину и repr, чтобы увидеть лишние символы)
print(f"[DEBUG] BOT_TOKEN len={len(clean_token)} repr={repr(clean_token)}", flush=True)

# 4) валидируем формат телеграмного токена
if not re.fullmatch(r"\d+:[A-Za-z0-9_\-]{35,}", clean_token):
    print("[FATAL] Invalid BOT_TOKEN. Fix env var BOT_TOKEN in Railway.", flush=True)
    sys.exit(1)

from aiogram import Bot
bot = Bot(clean_token)
BOT_TOKEN = os.getenv("BOT_TOKEN")
dp = Dispatcher()
bot = Bot(BOT_TOKEN)

with open(os.path.join(os.path.dirname(__file__), "presets.json"), "r", encoding="utf-8") as f:
    PRESETS = json.load(f)

USER_STATE = {}

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

def label_format(fmt_key:str)->str:
    return {"wav16":"WAV 16-bit","mp3_320":"MP3 320","wav24":"WAV 24-bit"}[fmt_key]

def analyze_lufs_and_tilt(path:str, sr_target=48000):
    y, sr = librosa.load(path, sr=sr_target, mono=True)
    y, _ = librosa.effects.trim(y, top_db=40)
    meter = pyln.Meter(sr)
    I = float(meter.integrated_loudness(y))
    S = np.abs(librosa.stft(y, n_fft=8192, hop_length=2048, window="hann"))**2
    freqs = librosa.fft_frequencies(sr=sr, n_fft=8192)
    psd = np.mean(S, axis=1) + 1e-18
    def band_mean(lo, hi):
        idx = np.where((freqs>=lo)&(freqs<hi))[0]
        return float(10*np.log10(np.mean(psd[idx]))) if idx.size>0 else 0.0
    hi = band_mean(8000, 12000)
    lo = band_mean(150, 300)
    tilt = hi - lo
    return I, tilt

def choose_presets_auto(I:float, tilt:float):
    if tilt <= -0.8: tone="bright"
    elif tilt >= 0.8: tone="warm"
    else: tone="balanced"
    if I <= -16.5: intensity="balanced"
    elif -16.5 < I <= -14.5: intensity="balanced"
    else: intensity="low"
    return intensity, tone

def build_ffmpeg_chain(inten_key: str, tone_key: str):
    inten = PRESETS["intensity"][inten_key]
    tone  = PRESETS["tone"][tone_key]

    eq_parts = []
    if tone.get("low_shelf"):
        lf = tone["low_shelf"]
        eq_parts.append(f"equalizer=f={lf['f']}:t=l:width={lf['width']}:g={lf['g']}")
    if tone.get("high_shelf"):
        hf = tone["high_shelf"]
        eq_parts.append(f"equalizer=f={hf['f']}:t=h:width={hf['width']}:g={hf['g']}")
    eq_chain = ",".join(eq_parts) if eq_parts else "anull"

    comp = inten["comp"]
    acompressor = f"acompressor=ratio={comp['ratio']}:threshold={comp['threshold_db']}dB:attack={comp['attack']}:release={comp['release']}:makeup=0"
    loudnorm = f"loudnorm=I={inten['I']}:TP={inten['TP']}:LRA={inten['LRA']}:print_format=summary"
    return f"{eq_chain},{acompressor},{loudnorm}"

def output_args(fmt_key:str):
    if fmt_key=="wav16":
        return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"
    if fmt_key=="wav24":
        return "-ar 48000 -ac 2 -c:a pcm_s24le", "mastered_uhd.wav"
    if fmt_key=="mp3_320":
        return "-ar 48000 -ac 2 -codec:a libmp3lame -b:a 320k", "mastered_320.mp3"
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav"

async def process_audio(in_path: str, out_path: str, intensity: str, tone: str, fmt_key: str):
    af = build_ffmpeg_chain(intensity, tone)
    fmt_args, _ = output_args(fmt_key)
    cmd = f'ffmpeg -y -i {shlex.quote(in_path)} -af "{af}" {fmt_args} {shlex.quote(out_path)}'
    proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg failed: " + err.decode("utf-8", errors="ignore"))

@dp.message(Command("start"))
async def start(m: Message):
    USER_STATE[m.from_user.id] = {"intensity": PRESETS["defaults"]["intensity"],
                                  "tone": PRESETS["defaults"]["tone"],
                                  "format": PRESETS["defaults"]["format"],
                                  "auto": False}
    await m.answer(
        "Йо! Я — Mr Mastering.\n"
        "Выбери пресеты или включи 🤖 Auto, затем пришли трек (.mp3/.wav).\n"
        "Можно выбрать формат вывода: WAV16 / MP3 320 / WAV24.",
        reply_markup=kb_main(m.from_user.id)
    )

@dp.callback_query(F.data == "menu_intensity")
async def menu_intensity(c):
    await c.message.edit_text("Выбери Intensity:", reply_markup=kb_intensity())
    await c.answer()

@dp.callback_query(F.data == "menu_tone")
async def menu_tone(c):
    await c.message.edit_text("Выбери Tone:", reply_markup=kb_tone())
    await c.answer()

@dp.callback_query(F.data == "menu_format")
async def menu_format(c):
    await c.message.edit_text("Выбери формат вывода:", reply_markup=kb_format())
    await c.answer()

@dp.callback_query(F.data == "back_main")
async def back_main(c):
    await c.message.edit_text("Главное меню:", reply_markup=kb_main(c.from_user.id))
    await c.answer()

@dp.callback_query(F.data.startswith("set_intensity_"))
async def set_intensity(c):
    val = c.data.replace("set_intensity_", "")
    USER_STATE[c.from_user.id]["intensity"] = val
    await c.message.edit_text(f"Intensity = {val}\nОкей, кинь аудио или настрой Tone/Format.", reply_markup=kb_main(c.from_user.id))
    await c.answer()

@dp.callback_query(F.data.startswith("set_tone_"))
async def set_tone(c):
    val = c.data.replace("set_tone_", "")
    USER_STATE[c.from_user.id]["tone"] = val
    await c.message.edit_text(f"Tone = {val}\nОкей, кинь аудио или настрой Intensity/Format.", reply_markup=kb_main(c.from_user.id))
    await c.answer()

@dp.callback_query(F.data.startswith("set_fmt_"))
async def set_fmt(c):
    key = c.data.replace("set_fmt_", "")
    mapping = {"wav16":"wav16","mp3_320":"mp3_320","wav24":"wav24"}
    key = mapping.get(key, "wav16")
    USER_STATE[c.from_user.id]["format"] = key
    await c.message.edit_text(f"Output = {label_format(key)}\nКинь аудио.", reply_markup=kb_main(c.from_user.id))
    await c.answer()

@dp.callback_query(F.data == "toggle_auto")
async def toggle_auto(c):
    st = USER_STATE.get(c.from_user.id, PRESETS["defaults"])
    st["auto"] = not st.get("auto", False)
    USER_STATE[c.from_user.id] = st
    await c.message.edit_text(("🤖 Auto включён.\nПришли аудио — выберу Intensity/Tone сам."
                               if st["auto"] else
                               "🤖 Auto выключен.\nВыбери пресеты руками и пришли аудио."),
                              reply_markup=kb_main(c.from_user.id))
    await c.answer()

@dp.message(F.audio | F.document)
async def on_audio(m: Message):
    file = m.audio or m.document
    if not file:
        return
    name = (file.file_name or "input").lower()
    if not (name.endswith(".mp3") or name.endswith(".wav")):
        await m.reply("Пришли .mp3 или .wav 🙏")
        return

    st = USER_STATE.get(m.from_user.id) or PRESETS["defaults"]
    inten = st["intensity"]; tone = st["tone"]; fmtk = st["format"]
    auto = st.get("auto", False)

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
            await m.reply_document(open(out_path, "rb"), caption=f"Готово ✅  Intensity={inten}, Tone={tone}, Format={label_format(fmtk)}")
    except Exception as e:
        await m.reply(f"Ошибка: {e}")

def main():
    if not BOT_TOKEN:
        raise SystemExit("Set BOT_TOKEN env var")
    print("Mr Mastering bot is running…")
    asyncio.run(dp.start_polling(bot))

if __name__ == "__main__":
    main()
