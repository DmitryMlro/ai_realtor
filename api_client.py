from __future__ import annotations
import json
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from config import cfg

BASE_URL = "YOUR_BASE_URL"
APARTMENTS_ENDPOINT = f"{BASE_URL}/END/POINT"

DEFAULT_SECTION = getattr(cfg, "listings_section", "secondary")


def _is_http(url: Optional[str]) -> bool:
    if not url:
        return False
    s = str(url).strip().lower()
    return s.startswith("http://") or s.startswith("https://")


def _clean_path(p: Optional[str]) -> Optional[str]:
    if not p:
        return None
    s = str(p).strip()
    if not s:
        return None
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("/"):
        s = s[1:]
    return s


def _photo_candidates_from_obj(obj: Dict[str, Any]) -> List[str]:
    cands: List[str] = []

    for key in ("name", "url", "mini"):
        val = obj.get(key)
        if not val:
            continue
        s = str(val).strip()

        if _is_http(s):
            cands.append(s)
            continue

        sp = _clean_path(s)
        if sp and sp.startswith("storage/"):
            cands.append(f"ENDPOINT:PORT/{sp}")
            cands.append(f"ENDPOINT/{sp}")
            cands.append(f"ENDPOINT/{sp}")
            continue

        if sp and (sp.lower().endswith(".jpg") or sp.lower().endswith(".jpeg") or sp.lower().endswith(".png")):
            cands.append(f"ENDPOINT/{sp}")

    seen = set()
    out: List[str] = []
    for u in cands:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _extract_photos_candidates(photos_field: Any) -> List[str]:
    cands: List[str] = []

    def from_list(lst: List[Any]) -> None:
        for entry in lst:
            if isinstance(entry, dict):
                cands.extend(_photo_candidates_from_obj(entry))
            elif isinstance(entry, str):
                s = entry.strip()
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        for obj in parsed:
                            if isinstance(obj, dict):
                                cands.extend(_photo_candidates_from_obj(obj))
                            elif isinstance(obj, str):
                                if _is_http(obj):
                                    cands.append(obj)
                                else:
                                    sp = _clean_path(obj)
                                    if sp and sp.startswith("storage/"):
                                        cands.append(f"ENDPOINT:PORT/{sp}")
                                        cands.append(f"ENDPOINT/{sp}")
                                        cands.append(f"ENDPOINT/{sp}")
                                    elif sp and (sp.lower().endswith(".jpg") or sp.lower().endswith(
                                            ".jpeg") or sp.lower().endswith(".png")):
                                        cands.append(f"ENDPOINT/{sp}")
                except Exception:
                    if _is_http(s):
                        cands.append(s)
                    else:
                        sp = _clean_path(s)
                        if sp and sp.startswith("storage/"):
                            cands.append(f"ENDPOINT:PORT/{sp}")
                            cands.append(f"ENDPOINT/{sp}")
                            cands.append(f"ENDPOINT/{sp}")
                        elif sp and (sp.lower().endswith(".jpg") or sp.lower().endswith(".jpeg") or sp.lower().endswith(
                                ".png")):
                            cands.append(f"ENDPOINT/{sp}")

    if not photos_field:
        return []

    if isinstance(photos_field, list):
        from_list(photos_field)
    elif isinstance(photos_field, str):
        s = photos_field.strip()
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                from_list(parsed)
        except Exception:
            if _is_http(s):
                cands.append(s)
    seen = set()
    out: List[str] = []
    for u in cands:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    candidates = _extract_photos_candidates(item.get("photos"))
    for alt_key in ("images", "image_urls", "gallery"):
        alt = item.get(alt_key)
        if isinstance(alt, list):
            for e in alt:
                if isinstance(e, str) and _is_http(e):
                    candidates.append(e)
                elif isinstance(e, dict):
                    candidates.extend(_photo_candidates_from_obj(e))
        elif isinstance(alt, str) and _is_http(alt):
            candidates.append(alt)

    seen = set()
    uniq: List[str] = []
    for u in candidates:
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)

    item["_photo_candidates"] = uniq
    return item


class ListingsAPI:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=25)
            headers = {
                "User-Agent": "AIRealtorBot/2.0",
                "Accept": "application/json",
            }
            self._session = aiohttp.ClientSession(timeout=timeout, headers=headers)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _post(self, url: str, json_body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        sess = await self._get_session()
        async with sess.post(url, json=json_body) as resp:
            status = resp.status
            try:
                data = await resp.json(content_type=None)
            except Exception:
                text = await resp.text()
                data = {"raw": text}
            return status, data

    def _payload_mode_a(self, filters: Dict[str, Any], limit: int, offset: int) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "key": cfg.api_key,
            "limit": limit,
            "offset": offset,
            "section": DEFAULT_SECTION,
        }

        if "microarea_id" in filters and filters["microarea_id"]:
            v = filters["microarea_id"]
            body["microarea_id"] = v if isinstance(v, list) else [v]

        if "district_id" in filters and filters["district_id"]:
            v = filters["district_id"]
            body["district_id"] = v if isinstance(v, list) else [v]

        if "rooms_in" in filters and filters["rooms_in"]:
            v = filters["rooms_in"]
            body["rooms_in"] = v if isinstance(v, list) else [v]

        if "price_max" in filters and filters["price_max"]:
            body["price_max"] = filters["price_max"]

        return body

    def _payload_mode_b(self, filters: Dict[str, Any], limit: int, offset: int) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "key": cfg.api_key,
            "limit": limit,
            "offset": offset,
            "section": DEFAULT_SECTION,
        }

        if "microarea_id" in filters and filters["microarea_id"]:
            body["microarea"] = filters["microarea_id"]

        if "district_id" in filters and filters["district_id"]:
            body["district"] = filters["district_id"]

        if "rooms_in" in filters and filters["rooms_in"]:
            body["rooms"] = filters["rooms_in"]

        if "price_max" in filters and filters["price_max"]:
            body["price_max"] = filters["price_max"]

        return body

    def _unpack(self, data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
        items = data.get("results") or data.get("items") or []
        total = data.get("total") or data.get("count") or len(items) or 0
        normed = [_normalize_item(dict(it)) for it in items if isinstance(it, dict)]
        return normed, int(total)

    async def get_apartments(self, filters: Dict[str, Any], limit: int = 3, offset: int = 0) -> Dict[str, Any]:
        payload_a = self._payload_mode_a(filters, limit, offset)
        try:
            print(f"[API] Trying mode A: *_in + *_id payload={payload_a}")
            st, data = await self._post(APARTMENTS_ENDPOINT, payload_a)
            if st == 200:
                items, total = self._unpack(data)
                print(f"[API] A OK: items={len(items)} total={total}")
                return {"results": items, "total": total}
            else:
                print(f"[API] HTTP {st}: {data}")
                raise RuntimeError("HTTP 400")
        except Exception as e:
            print(f"[API] A: *_in + *_id error: {e}")

        payload_b = self._payload_mode_b(filters, limit, offset)
        print(f"[API] Trying mode B: singular keys payload={payload_b}")
        st, data = await self._post(APARTMENTS_ENDPOINT, payload_b)
        if st != 200:
            raise RuntimeError(f"HTTP {st}: {data}")

        items, total = self._unpack(data)
        print(f"[API] B: singular keys OK: items={len(items)} total={total}")
        if items and cfg.debug:
            sample = items[0].get("_photo_candidates", [])[:3]
            if sample:
                print(f"[API] B photos sample: {sample}")
        return {"results": items, "total": total}
