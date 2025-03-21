import numpy as np
from services.common_lib.config import settings
from typing import Dict, Optional, Tuple

def check_threshold(count: int) -> bool:
    return count >= settings.PRIVACY_MIN_THRESHOLD

def apply_differential_privacy(count: int) -> float:
    epsilon = settings.PRIVACY_NOISE_EPSILON
    sensitivity = 1.0
    scale = sensitivity / epsilon
    noise = np.random.laplace(0, scale, 1)[0]
    return max(0.0, count + noise)

def parse_privacy_signals(
    tcf_string: Optional[str],
    us_privacy_string: Optional[str],
    cross_device_bridging: bool
) -> bool:
    """
    Parse privacy signals and determine if bridging is allowed.
    Returns True if bridging is allowed based on privacy signals.
    """
    # If no privacy signals provided, use consent flags
    if not tcf_string and not us_privacy_string:
        return cross_device_bridging

    # Parse TCF string if provided
    tcf_allowed = True
    if tcf_string:
        try:
            # Basic TCF string validation
            if len(tcf_string) < 10:  # Minimum length for valid TCF string
                tcf_allowed = False
            else:
                # Check for required consent bits
                # This is a simplified check - in production you'd want to use a proper TCF parser
                tcf_allowed = tcf_string[0] == 'C'  # 'C' indicates consent given
        except Exception:
            tcf_allowed = False

    # Parse US Privacy string if provided
    us_allowed = True
    if us_privacy_string:
        try:
            # Basic US Privacy string validation (CCPA)
            if len(us_privacy_string) != 4:  # CCPA string should be 4 characters
                us_allowed = False
            else:
                # Check if user has not opted out
                # Format: [Version][Notice][Sale][LSPA]
                us_allowed = us_privacy_string[2] in ['N', 'Y']  # 'N' or 'Y' means not opted out
        except Exception:
            us_allowed = False

    # Both signals must allow bridging if both are present
    if tcf_string and us_privacy_string:
        return tcf_allowed and us_allowed and cross_device_bridging
    elif tcf_string:
        return tcf_allowed and cross_device_bridging
    elif us_privacy_string:
        return us_allowed and cross_device_bridging
    
    return cross_device_bridging
