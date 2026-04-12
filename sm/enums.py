# sm/enums.py

from enum import Enum


class Tone(str, Enum):
    WARM = "warm"
    BALANCED = "balanced"
    BRIGHT = "bright"


class Intensity(str, Enum):
    LOW = "low"
    BALANCED = "balanced"
    HIGH = "high"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class AnchorState(str, Enum):
    DEFICIENT = "deficient"
    BALANCED = "balanced"
    EXCESSIVE = "excessive"


class BridgeState(str, Enum):
    BROKEN = "broken"
    BALANCED = "balanced"
    OVERGLUED = "overglued"


class CleanupReadiness(str, Enum):
    DENIED = "denied"
    GUARDED = "guarded"
    SAFE = "safe"


class UpperBodyShape(str, Enum):
    NATURAL = "natural"
    BOXY = "boxy"


class TransitionState(str, Enum):
    STABLE = "stable"
    WEAK = "weak"
    THINNING = "thinning"


class ProjectionReadiness(str, Enum):
    DENIED = "denied"
    GUARDED = "guarded"
    READY = "ready"


class ProjectionState(str, Enum):
    UNDERPROJECTED = "underprojected"
    BALANCED = "balanced"
    OVERPUSHED = "overpushed"


class RoleName(str, Enum):
    ANCHOR = "anchor"
    BRIDGE = "bridge"
    CLEANUP = "cleanup"
    GUARD = "guard"
    PROJECTION = "projection"
    SPARK = "spark"
