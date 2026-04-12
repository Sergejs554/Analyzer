# sm/precondition.py

def build_neutral_preclean_chain(enable_afftdn: bool = False) -> str:
    parts = ["highpass=f=25:width=0.7"]
    if enable_afftdn:
        parts.append("afftdn=nf=-25")
    return ",".join(parts)
