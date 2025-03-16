def build_eids(bridging_confidence: float, user_id: str, bridging_method: str, source: str) -> list:
    if bridging_confidence < 0.8:
        return []
    mm_map = {
        "cookieSync": "2",
        "crossDomainDeterministic": "3",
        "crossDomainProbabilistic": "5"
    }
    mm_value = mm_map.get(bridging_method, "3")
    atype = "3" if bridging_method.startswith("crossDomain") else "1"

    eid = {
        "inserter": "householdiq",
        "matcher": "householdiq",
        "source": source,
        "mm": mm_value,
        "uids": [
            {
                "id": user_id,
                "atype": atype
            }
        ]
    }
    return [eid]
