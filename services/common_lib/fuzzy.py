import Levenshtein

def fuzzy_similarity(str_a: str, str_b: str) -> float:
    if not str_a or not str_b:
        return 0.0
    distance = Levenshtein.distance(str_a.lower(), str_b.lower())
    max_len = max(len(str_a), len(str_b))
    if max_len == 0:
        return 1.0
    return 1.0 - float(distance) / float(max_len)
