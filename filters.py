from typing import Dict, Any, List

REQUIRED_KEYS = ["district_id", "price_max", "rooms_in", "area_min"]

def missing_keys(filters: Dict[str, Any]) -> List[str]:
    data = filters or {}
    return [k for k in REQUIRED_KEYS if data.get(k) in (None, "", [], {})]

def merge_change(filters: Dict[str, Any], parsed: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(filters or {})
    for k, v in (parsed or {}).items():
        if v not in (None, "", [], {}):
            out[k] = v
    return out
