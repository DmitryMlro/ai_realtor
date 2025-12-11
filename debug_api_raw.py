import asyncio
import json
from typing import Any, Dict, List

import aiohttp

from config import cfg

BASE_URL = "ENDPOINT:PORT"
APARTMENTS_ENDPOINT = f"{BASE_URL}/way/endpoint"


async def fetch_raw_api(payload: Dict[str, Any]) -> Dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=25)
    headers = {
        "User-Agent": "AIRealtorBotDebug/2.0",
        "Accept": "application/json",
    }
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.post(APARTMENTS_ENDPOINT, json=payload) as resp:
            status = resp.status
            print(f"[DEBUG] HTTP status: {status}")
            try:
                data = await resp.json(content_type=None)
            except Exception:
                text = await resp.text()
                print("[DEBUG] Raw text response:")
                print(text)
                return {}
            print("[DEBUG] Top-level keys:", list(data.keys()))
            return data


def _pick_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = data.get("results") or data.get("items") or []
    if not isinstance(items, list):
        return []
    return [it for it in items if isinstance(it, dict)]


async def main() -> None:
    payload = {
        "key": cfg.api_key,
        "limit": 3,
        "offset": 0,
        "section": "secondary",
        "microarea_id": [97],
        "rooms_in": [2],
        "price_max": 100000,
    }

    print("[DEBUG] Request payload:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    data = await fetch_raw_api(payload)
    if not data:
        print("[DEBUG] No data from API")
        return

    items = _pick_items(data)
    print(f"[DEBUG] Total items in response: {len(items)}")

    if not items:
        return

    for idx, item in enumerate(items[:3]):
        print("\n" + "=" * 80)
        print(f"[DEBUG] ITEM #{idx}")
        print("[DEBUG] Keys:", list(item.keys()))

        for key in [
            "photos", "images", "image_urls", "gallery",
            "photos_urls", "photos_url", "image_url",
            "photo", "cover", "preview",
        ]:
            if key in item:
                print(f"\n[DEBUG] Field `{key}`:")
                try:
                    print(json.dumps(item[key], ensure_ascii=False, indent=2))
                except TypeError:
                    print(repr(item[key]))

        print("\n[DEBUG] Full item JSON (pretty):")
        try:
            print(json.dumps(item, ensure_ascii=False, indent=2))
        except TypeError:
            print(repr(item))


if __name__ == "__main__":
    asyncio.run(main())
