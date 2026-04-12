# sm/entry.py

from .render import render_sm_core_v1


def render_sm_branch_v1(
    input_path: str,
    tone: str,
    intensity: str,
    fmt: str,
    td: str,
    use_neutral_preclean: bool = True,
    enable_afftdn: bool = False,
):
    return render_sm_core_v1(
        input_path=input_path,
        tone=tone,
        intensity=intensity,
        fmt=fmt,
        td=td,
        use_neutral_preclean=use_neutral_preclean,
        enable_afftdn=enable_afftdn,
    )
