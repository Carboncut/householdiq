import numpy as np
from services.common_lib.config import settings

def check_threshold(count: int) -> bool:
    return count >= settings.PRIVACY_MIN_THRESHOLD

def apply_differential_privacy(count: int) -> float:
    epsilon = settings.PRIVACY_NOISE_EPSILON
    sensitivity = 1.0
    scale = sensitivity / epsilon
    noise = np.random.laplace(0, scale, 1)[0]
    return max(0.0, count + noise)
