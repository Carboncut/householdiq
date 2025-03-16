import random
from services.common_lib.config import settings

def should_sample_event(event_type: str) -> bool:
    """
    Returns True if an event of this type should be sampled (collected) for anonymized analysis,
    based on the sampling rate in settings.
    """
    rate = settings.SAMPLING_RATES.get(event_type, 10000)
    return (random.randint(1, rate) == 1)
