import asyncio
import json

from api_client import ListingsAPI, APARTMENTS_ENDPOINT
from config import cfg

API_KEY = getattr(cfg, "api_key", None) or getattr(cfg, "listings_api_key", None)
SECTION = "secondary"


async def main():
    api = ListingsAPI()

    payload = {
        "key": API_KEY,
        "section": SECTION,
        "limit": 20,
        "offset": 0,
    }
    print("Payload:", payload)

    status, data = await api._post(APARTMENTS_ENDPOINT, payload)
    print("HTTP Status:", status)

    if isinstance(data, dict):
        print("Top-level keys:", list(data.keys()))

    print("\nRaw JSON (до ~1000 символів):")
    print(json.dumps(data, ensure_ascii=False, indent=2)[:1000])

    sess = getattr(api, "_session", None)
    if sess and not sess.closed:
        await sess.close()


if __name__ == "__main__":
    asyncio.run(main())
