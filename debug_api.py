import asyncio
import json

from api_client import ListingsAPI, APARTMENTS_ENDPOINT
from config import cfg


def get_api_key():
    return getattr(cfg, "api_key", None) or getattr(cfg, "listings_api_key", None)


async def test_sections(api: ListingsAPI):
    base_key = get_api_key()

    sections_to_try = [
        "sale",
        "rent",
        "buy",
        "sell",
        "secondary",
        "primary",
        "long",
        "short",
        1,
        2,
        3,
    ]

    for s in sections_to_try:
        payload = {
            "key": base_key,
            "limit": 3,
            "offset": 0,
            "section": s,
        }
        print("\n==============================")
        print(f"TEST section={s!r}")
        print("Payload:", payload)

        status, data = await api._post(APARTMENTS_ENDPOINT, payload)
        print("HTTP Status:", status)

        # красивий вивід
        if isinstance(data, dict):
            print("API keys:", list(data.keys()))
            print("Raw data:", json.dumps(data, ensure_ascii=False, indent=2)[:800])
        else:
            print("Raw data:", str(data)[:800])


async def main():
    api = ListingsAPI()
    await test_sections(api)

    sess = getattr(api, "_session", None)
    if sess and not sess.closed:
        await sess.close()


if __name__ == "__main__":
    asyncio.run(main())
