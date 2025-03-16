import hashlib
from services.common_lib.config import settings

def hash_sha256(value: str) -> str:
    salted_value = f"{settings.GLOBAL_SALT}-{value}"
    return hashlib.sha256(salted_value.encode("utf-8")).hexdigest()
