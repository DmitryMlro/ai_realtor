import os
from dataclasses import dataclass
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def _clean(val: Optional[str]) -> str:
    if val is None:
        return ""
    v = val.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    return v

def _get(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None:
            return _clean(v)
    return default

def _int(*names: str, default: int) -> int:
    for n in names:
        v = os.getenv(n)
        if v is not None:
            try:
                return int(_clean(v))
            except Exception:
                return default
    return default

def _bool(*names: str, default: bool = False) -> bool:
    truthy = {"1","true","yes","y","on"}
    for n in names:
        v = os.getenv(n)
        if v is not None:
            return _clean(v).lower() in truthy
    return default

@dataclass
class Cfg:
    bot_token: str
    supabase_url: str
    supabase_anon_key: str
    api_base: str
    api_key: str
    api_timeout: int
    api_endpoint: str
    api_mode: str
    sheets_id: str
    gs_service_account_json_path: str
    limit_per_page: int
    texts_ttl_seconds: int
    debug: bool

cfg = Cfg(
    bot_token=_get("BOT_TOKEN", "TELEGRAM_BOT_TOKEN"),
    supabase_url=_get("SUPABASE_URL"),
    supabase_anon_key=_get("SUPABASE_ANON_KEY"),
    api_base=_get("LISTINGS_API_BASE", "API_BASE", default="ENDPOINT:PORT").rstrip("/"),
    api_key=_get("LISTINGS_API_KEY", "API_KEY"),
    api_timeout=_int("API_TIMEOUT", default=20),
    api_endpoint=_get("LISTINGS_API_ENDPOINT", default="/api/get_apartments"),
    api_mode=_get("LISTINGS_API_MODE", default="adaptive"),
    sheets_id=_get("GS_SPREADSHEET_ID", "SHEETS_ID"),
    gs_service_account_json_path=_get("GS_SERVICE_ACCOUNT_JSON_PATH", default="credentials/service_account.json"),
    limit_per_page=_int("LIMIT_PER_PAGE", default=3),
    texts_ttl_seconds=_int("TEXTS_TTL_SECONDS", default=900),
    debug=_bool("DEBUG", default=False),
)

def validate_config():
    tok = cfg.bot_token
    if not tok:
        raise RuntimeError("BOT_TOKEN EMPTY")
    if ":" not in tok or not tok.split(":", 1)[0].isdigit():
        raise RuntimeError("BOT_TOKEN HAVE <digits>:<token>")
