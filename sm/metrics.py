# sm/metrics.py

from .contracts import AnalysisMetrics


def collect_sm_metrics(input_path: str) -> AnalysisMetrics:
    """
    Здесь потом подключим твои реальные метрики из analyze_mastering / auto_analysis.
    Пока только правильная точка входа.
    """
    return AnalysisMetrics()
