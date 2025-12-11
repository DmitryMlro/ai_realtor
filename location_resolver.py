from __future__ import annotations
from typing import Dict, Any

class LocationResolver:
    def __init__(self, districts_data: Dict[str, Dict[str, str]]):
        self.districts = districts_data.get("district", {}) or {}
        self.microareas = districts_data.get("microarea", {}) or {}
        self.streets = districts_data.get("street", {}) or {}

    def resolve(self, text: str) -> Dict[str, Any]:
        t = (text or "").strip().lower()
        if not t:
            return {}

        for name, sid in self.streets.items():
            if t.startswith(name.lower()):
                return {"street_id": sid}

        for name, mid in self.microareas.items():
            if t.startswith(name.lower()):
                return {"microarea_id": mid}

        for name, did in self.districts.items():
            if t.startswith(name.lower()):
                return {"district_id": did}

        return {}
