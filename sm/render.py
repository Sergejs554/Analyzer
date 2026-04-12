# sm/render.py

from .analysis import analyze_sm_input
from .selector import select_sm_profiles
from .router import build_sm_router_summary
from .contracts import SmartMasterDebugBundle


def render_sm_core_v1(
    input_path: str,
    tone: str,
    intensity: str,
    fmt: str,
    td: str,
    use_neutral_preclean: bool = True,
):
    analysis = analyze_sm_input(input_path)
    selection = select_sm_profiles(analysis, tone, intensity)
    router = build_sm_router_summary(selection)

    # На первом этапе здесь просто возвращаем debug bundle.
    # Потом сюда добавим:
    # 1. optional neutral preclean render
    # 2. real DSP role assembly
    # 3. final temp render
    return SmartMasterDebugBundle(
        analysis=analysis,
        selection=selection,
        router=router,
    )
