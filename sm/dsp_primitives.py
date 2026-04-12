# sm/dsp_primitives.py

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def build_eq_bell(f_hz: float, gain_db: float, q_width: float) -> str:
    return f"equalizer=f={f_hz}:t=q:w={q_width}:g={gain_db}"


def build_highshelf(f_hz: float, gain_db: float) -> str:
    return f"highshelf=f={f_hz}:g={gain_db}"


def build_highpass(f_hz: float) -> str:
    return f"highpass=f={f_hz}:width=0.707"


def build_lowpass(f_hz: float) -> str:
    return f"lowpass=f={f_hz}:width=0.707"
