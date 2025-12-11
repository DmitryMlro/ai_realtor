from __future__ import annotations
import asyncio
import random
import re
from typing import Dict, Any, List, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode, ChatAction
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.types import (
    Message, ContentType,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InputMediaPhoto, BufferedInputFile
)

import aiohttp

from config import cfg, validate_config
from sheets_client import SheetsClient, append_booking
from api_client import ListingsAPI
from supabase_client import SupabaseClient
from parsers import (
    parse_free_text,
    DISTRICT_LABELS,
    MICROAREA_LABELS,
)

# init
validate_config()
bot = Bot(token=cfg.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

sheets = SheetsClient(cfg.sheets_id)
api = ListingsAPI()
supa = SupabaseClient()

WELCOME_AFTER_NAME = "Дуже приємно познайомитись, {name}. Щоб бути максимально корисним для вас, я задам декілька запитань."

WELCOME_TEXT: Optional[str] = None
QUESTIONS: List[Dict[str, Any]] = []
ORDER_KEYS: List[str] = []
KEY_TO_TEXT: Dict[str, str] = {}

CANON: Dict[str, List[str]] = {
    "name":      ["name"],
    "type":      ["type", "property_type", "object_type"],
    "district":  ["district_id", "microarea_id", "district_text", "district", "location", "area", "district_area", "rayon"],
    "rooms":     ["rooms_in", "rooms", "room_count", "rooms_count"],
    "condition": ["condition_in", "state", "condition", "repair", "remont"],
    "budget":    ["budget", "price_max", "max_price", "budget_max", "price"],
}

# aiohttp session for photos
_http_session: Optional[aiohttp.ClientSession] = None

async def _get_http() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=25)
        headers = {"User-Agent": "AIRealtorBot/2.0", "Accept": "*/*"}
        _http_session = aiohttp.ClientSession(timeout=timeout, headers=headers)
    return _http_session

# helpers
async def _typing(msg: Message, min_s: float = 2.0, max_s: float = 3.0):
    try:
        await bot.send_chat_action(chat_id=msg.chat.id, action=ChatAction.TYPING)
    except Exception:
        pass
    await asyncio.sleep(random.uniform(min_s, max_s))

def _ensure_loaded():
    global WELCOME_TEXT, QUESTIONS, ORDER_KEYS, KEY_TO_TEXT
    if WELCOME_TEXT is None:
        WELCOME_TEXT = sheets.get_welcome("ukrainian")
        QUESTIONS = sheets.get_questions()
        ORDER_KEYS = [r["question_key"] for r in QUESTIONS]
        KEY_TO_TEXT = {r["question_key"]: r["question_text"] for r in QUESTIONS}

def _all_questions_except_name() -> List[str]:
    return [k for k in ORDER_KEYS if k != "name"]

def _is_answered(qkey: str, answers: Optional[Dict[str, Any]]) -> bool:
    if not answers:
        return False

    for k in CANON.get(qkey, [qkey]):
        v = answers.get(k)
        if v not in (None, "", [], {}):
            return True

    qtext = (KEY_TO_TEXT.get(qkey) or "").lower()

    if any(w in qtext for w in ["район", "локац", "таїров", "центр", "фонтан", "аркаді", "мікрорайон"]):
        return any(answers.get(k) not in (None, "", [], {}) for k in CANON["district"])

    if any(w in qtext for w in ["кімнат", "комнат", "к-ть кімнат", "скільки кімнат"]):
        return any(answers.get(k) not in (None, "", [], {}) for k in CANON["rooms"])

    if any(w in qtext for w in ["ремонт", "стан", "оздоб", "отделоч"]):
        return any(answers.get(k) not in (None, "", [], {}) for k in CANON["condition"])

    if any(w in qtext for w in ["бюджет", "ціна", "цiна", "price", "варт", "скільки готові"]):
        return any(answers.get(k) not in (None, "", [], {}) for k in CANON["budget"])

    if any(w in qtext for w in ["квартир", "будин", "тип", "що ви бажаєте придбати"]):
        return any(answers.get(k) not in (None, "", [], {}) for k in CANON["type"])

    return False


def _missing_now(answers: Optional[Dict[str, Any]]) -> List[str]:
    answers = answers or {}
    return [k for k in _all_questions_except_name() if not _is_answered(k, answers)]


def _bulleted(keys: List[str]) -> str:
    lines = ["Розкажіть, будь ласка, що саме шукаєте:"]
    for k in keys:
        q = KEY_TO_TEXT.get(k)
        if q:
            lines.append(f"• {q}")
    return "\n".join(lines)

def _norm_simple(s: str) -> str:
    s = (s or "").lower().strip()
    repl = {
        "ё": "е",
        "ї": "и",
        "і": "и",
        "є": "е",
        "ґ": "г",
        "ъ": "",
        "ь": "",
    }
    for a, b in repl.items():
        s = s.replace(a, b)
    s = re.sub(r"[.,;:!?()\[\]\-_/\\]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _detect_condition_value(text: str) -> Optional[int]:
    norm = _norm_simple(text)
    if not norm:
        return None

    POSITIVE_TOKENS = [
        "з ремонтом", "с ремонтом",
        "новый ремонт", "новий ремонт",
        "свежий ремонт", "качественный ремонт",
        "капремонт", "капитальный ремонт",
        "отличный ремонт", "евроремонт",
    ]

    NEGATIVE_TOKENS = [
        "без ремонта", "без ремонту",
        "после строител", "після буд",
        "состояние от строителей", "сост от строителей",
        "от строителей",
        "чернов", "чорнов",
        "под ремонт", "под ремонт",
    ]

    pos = any(t in norm for t in POSITIVE_TOKENS)
    neg = any(t in norm for t in NEGATIVE_TOKENS)

    if pos and not neg:
        return 8
    if neg and not pos:
        return 9
    if not pos and not neg:
        return None

    def last_index(tokens: List[str]) -> int:
        idx = -1
        for t in tokens:
            i = norm.rfind(t)
            if i > idx:
                idx = i
        return idx

    last_pos = last_index(POSITIVE_TOKENS) if pos else -1
    last_neg = last_index(NEGATIVE_TOKENS) if neg else -1

    if last_pos > last_neg:
        return 8
    if last_neg > last_pos:
        return 9
    return None



def _detect_location_ids(norm_text: str) -> Dict[str, int]:
    res: Dict[str, int] = {}

    def match_from_labels(labels: Dict[int, str], target_key: str) -> bool:
        for raw_id, label in (labels or {}).items():
            if not isinstance(label, str):
                continue

            ln = _norm_simple(label)
            tokens = [t for t in ln.split() if t]

            for token in tokens:
                if len(token) < 4:
                    continue

                base4 = token[:4]
                base5 = token[:5]

                if (base5 and base5 in norm_text) or (base4 and base4 in norm_text):
                    try:
                        res[target_key] = int(raw_id)
                        return True
                    except Exception:
                        continue
        return False

    try:
        if match_from_labels(MICROAREA_LABELS, "microarea_id"):
            return res
    except Exception as e:
        if cfg.debug:
            print(f"[location] microarea match error: {e}")

    try:
        if match_from_labels(DISTRICT_LABELS, "district_id"):
            return res
    except Exception as e:
        if cfg.debug:
            print(f"[location] district match error: {e}")

    return res


def _parse_into_answers(
    text: str,
    answers: Dict[str, Any],
    old_filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    out = dict(answers or {})
    norm = _norm_simple(text)

    found: Dict[str, Any] = {}
    try:
        found = parse_free_text(text) or {}
    except Exception as e:
        if cfg.debug:
            print(f"[parse_free_text] error: {e}")
        found = {}

    for k, v in (found.items() if isinstance(found, dict) else []):
        if v in (None, "", [], {}):
            continue

        if k == "price_max" and not out.get("budget"):
            out["budget"] = v

        # map rooms_in > rooms
        if k == "rooms_in" and not out.get("rooms"):
            out["rooms"] = v

        if k == "type":
            vv = str(v).lower()
            if "буд" in vv:
                out["type"] = "house"
            elif "кварт" in vv:
                out["type"] = "apartment"
            else:
                out["type"] = v
            continue

        out[k] = v

    if not out.get("district_text"):
        short = text.strip()
        if 0 < len(short.split()) <= 6:
            out["district_text"] = short

    if not out.get("budget") and not out.get("price_max"):
        val = None
        m = re.search(r"(\d+)\s*([kк]|тис|тыс)", norm)
        if m:
            try:
                val = int(m.group(1)) * 1000
            except Exception:
                val = None

        if val is None:
            m2 = re.search(r"(\d[\d\s]{3,})", text)
            if m2:
                try:
                    raw = re.sub(r"\s+", "", m2.group(1))
                    val = int(raw)
                except Exception:
                    val = None

        if val is not None:
            out["budget"] = val

    if old_filters and (not out.get("budget") and not out.get("price_max")):
        old_b = old_filters.get("price_max")
        if isinstance(old_b, (int, float)) and old_b > 0:
            if any(w in norm for w in ["дешевш", "дешевл", "дешев", "дешевее", "дешевле"]):
                out["budget"] = int(old_b * 0.9)
            elif any(w in norm for w in ["дорожч", "дороже", "подороже"]):
                out["budget"] = int(old_b * 1.1)

    if not out.get("rooms_in") and not out.get("rooms"):
        rooms_val: Optional[int] = None

        m = re.search(r"\b(\d+)\s*(к|комн|комнат|кімн|комнаты)\b", norm)
        if m:
            try:
                rooms_val = int(m.group(1))
            except Exception:
                rooms_val = None

        if rooms_val is None:
            if any(sub in norm for sub in ["однокімнат", "однокомнат", "одн комнат", "одно кімнат"]):
                rooms_val = 1
            elif any(sub in norm for sub in ["двокімнат", "двухкомнат", "двух комнат", "две комнат", "двохкімнат"]):
                rooms_val = 2
            elif any(sub in norm for sub in ["трикімнат", "трьохкімнат", "трехкомнат", "три комнат"]):
                rooms_val = 3

        if rooms_val is None:
            if any(w in norm for w in ["двушка", "двушк", "двойк", "двоечк"]):
                rooms_val = 2
            elif any(w in norm for w in ["трешка", "трьошк", "трешк", "трёшк"]):
                rooms_val = 3

        if rooms_val is not None:
            out["rooms_in"] = rooms_val
            out.setdefault("rooms", rooms_val)

    loc_ids = _detect_location_ids(norm)
    if loc_ids:
        for k, v in loc_ids.items():
            out[k] = v

    condition_keywords = [
        "ремонт", "без ремонт", "без ремонта",
        "чернов", "чорнов",
        "під ремонт", "под ремонт",
        "після будівель", "после строител",
        "от строител", "від будівельник", "вид будівельник",
        "отделоч", "оздоблювальн",
        "евроремонт", "євроремонт",
    ]

    if any(w in norm for w in condition_keywords):
        ci = _detect_condition_value(text)
        if ci is not None:
            out["condition_in"] = ci

    if not out.get("type"):
        if any(w in norm for w in ["дом", "будинок", "частный дом", "частн дом"]):
            out["type"] = "house"
        elif any(w in norm for w in ["квартир", "апартам", "квартира"]):
            out["type"] = "apartment"

    return out


def _filters_from_answers(ans: Dict[str, Any]) -> Dict[str, Any]:
    f: Dict[str, Any] = {}
    if ans.get("district_id"):
        try: f["district_id"] = int(ans["district_id"])
        except Exception: pass
    if ans.get("microarea_id"):
        try: f["microarea_id"] = int(ans["microarea_id"])
        except Exception: pass
    if ans.get("rooms_in") or ans.get("rooms"):
        try:
            f["rooms_in"] = int(ans.get("rooms_in") or ans.get("rooms"))
        except Exception:
            pass
    if ans.get("budget") or ans.get("price_max"):
        try:
            f["price_max"] = int(ans.get("budget") or ans.get("price_max"))
        except Exception:
            pass
    if ans.get("condition_in"):
        try:
            f["condition_in"] = int(ans["condition_in"])
        except Exception:
            pass
    t = (ans.get("type") or "").strip().lower()
    if t in {"house", "apartment"}:
        f["type"] = t
        f["object_type"] = t
        f["property_type"] = t
    return f

def _label_for_district(did: Optional[int]) -> Optional[str]:
    try:
        if did is None:
            return None
        return DISTRICT_LABELS.get(int(did))
    except Exception:
        return None

def _label_for_microarea(mid: Optional[int]) -> Optional[str]:
    try:
        if mid is None:
            return None
        return MICROAREA_LABELS.get(int(mid))
    except Exception:
        return None

def _filters_human(answers: Dict[str, Any], filters: Dict[str, Any]) -> str:
    parts = []
    t = (answers.get("type") or "").strip()
    if t:
        parts.append("Будинок" if t == "house" else "Квартира")
    r = answers.get("rooms_in") or answers.get("rooms")
    if r:
        try:
            parts.append(f"{int(r)}к")
        except Exception:
            parts.append(f"{r}к")
    loc = []
    if filters.get("microarea_id"):
        n = _label_for_microarea(filters["microarea_id"])
        if n:
            loc.append(n)
    if filters.get("district_id"):
        n = _label_for_district(filters["district_id"])
        if n:
            loc.append(n)
    if loc:
        parts.append(", ".join(loc))
    if filters.get("price_max"):
        parts.append(f"до ${int(filters['price_max']):,}".replace(",", " "))
    if (answers.get("condition_in") or filters.get("condition_in")) == 8:
        parts.append("з ремонтом")
    elif (answers.get("condition_in") or filters.get("condition_in")) in (9, 18):
        parts.append("без ремонту")
    return " · ".join(parts) or "Підбір за фільтрами"

def _filters_json_for_sheet(answers: Dict[str, Any], filters: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(filters)
    if out.get("district_id"):
        out["district"] = _label_for_district(out["district_id"])
    if out.get("microarea_id"):
        out["microarea"] = _label_for_microarea(out["microarea_id"])
    if answers.get("type"):
        out["type"] = "house" if answers["type"] == "house" else "apartment"
    if answers.get("condition_in") or filters.get("condition_in"):
        ci = answers.get("condition_in") or filters.get("condition_in")
        out["condition"] = "з ремонтом" if ci == 8 else "без ремонту"
    return out

def _filters_diff_human(new: Dict[str, Any]) -> Optional[str]:
    parts: List[str] = []

    name = None
    mid = new.get("microarea_id")
    if mid:
        name = _label_for_microarea(mid)
    if not name:
        did = new.get("district_id")
        if did:
            name = _label_for_district(did)
    if name:
        parts.append(name)

    rooms = new.get("rooms_in")
    if rooms:
        try:
            r = int(rooms)
        except Exception:
            r = None
        if r is None:
            parts.append(f"{rooms} кімнат")
        else:
            if r == 1:
                parts.append("1 кімната")
            elif 2 <= r <= 4:
                parts.append(f"{r} кімнати")
            else:
                parts.append(f"{r} кімнат")

    ci = new.get("condition_in")
    if ci == 8:
        parts.append("з ремонтом")
    elif ci in (9, 18):
        parts.append("без ремонту")

    price = new.get("price_max")
    if price:
        try:
            p = int(price)
            parts.append(f"до {p:,}$".replace(",", " "))
        except Exception:
            parts.append(f"до {price}$")

    if not parts:
        return None
    return ", ".join(parts)


def _format_address(item: Dict[str, Any]) -> Optional[str]:
    for k in ("address", "location", "addr"):
        if isinstance(item.get(k), str) and item[k].strip():
            return item[k].strip()
    addr = item.get("address")
    if isinstance(addr, dict):
        city = addr.get("city")
        street = " ".join(x for x in (addr.get("street_type"), addr.get("street")) if x)
        house = addr.get("house") or addr.get("house_number")
        parts = [p for p in (city, street, house) if p]
        if parts:
            return ", ".join(parts)
    return None

def _item_text_blob(item: Dict[str, Any]) -> str:
    parts: List[str] = []

    for key in ("title", "name", "headline"):
        v = item.get(key)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())

    addr = _format_address(item)
    if addr:
        parts.append(addr)

    for key in ("description", "short_description", "body", "comment", "notes"):
        v = item.get(key)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())

    blob = "\n".join(parts)
    return _norm_simple(blob)


def _has_repair(blob_norm: str) -> bool:
    patterns = [
        "z remontom", "s remontom", "з ремонтом", "с ремонтом",
        "novyi remont", "noviy remont", "novij remont",
        "новий ремонт", "новыи ремонт", "kapitaln", "капитальн", "капитальный ремонт",
        "evroremont", "евроремонт", "sviezhi remont", "свежий ремонт", "хороший ремонт", "хороший ремонт",
    ]
    return any(p in blob_norm for p in patterns)


def _is_without_repair(blob_norm: str) -> bool:
    patterns = [
        "bez remonta", "без ремонта", "без ремонту", "бес ремонта", "бес ремонту",
        "posle stroitel", "після буд", "posle stroit", "ot stroitelei",
        "vid stroitelei", "от строителей", "от застройщика",
        "chernov", "чорнов", "чернов", "chernovoj", "черновой", "чорновий",
    ]
    return any(p in blob_norm for p in patterns)

def _extract_description(item: Dict[str, Any]) -> Optional[str]:
    candidates: List[str] = []

    for key in (
        "description",
        "description_full",
        "full_description",
        "short_description",
        "body",
        "text",
        "comment",
        "note",
    ):
        val = item.get(key)
        if isinstance(val, str):
            s = val.strip()
            if s:
                candidates.append(s)

    if not candidates:
        for k, v in item.items():
            if isinstance(v, str) and "descr" in k.lower():
                s = v.strip()
                if s:
                    candidates.append(s)

    if not candidates:
        return None

    desc = max(candidates, key=len)
    return desc.strip()


async def _try_fetch_bytes(url: str) -> Optional[bytes]:
    try:
        sess = await _get_http()
        async with sess.get(url, allow_redirects=True) as resp:
            ct = (resp.headers.get("Content-Type") or "").lower()
            if resp.status == 200 and ct.startswith("image/"):
                return await resp.read()
            if resp.status == 200 and not ct:
                data = await resp.read()
                if data and len(data) > 128:
                    return data
    except Exception as e:
        if cfg.debug:
            print(f"[PHOTOS] fetch error {url}: {e}")
    return None

async def _fetch_first_n_photos(item: Dict[str, Any], max_count: int = 5) -> List[BufferedInputFile]:
    files: List[BufferedInputFile] = []
    cands = item.get("_photo_candidates") or []
    used: set[str] = set()

    for url in cands:
        if url in used:
            continue
        used.add(url)
        b = await _try_fetch_bytes(url)
        if b:
            name = url.split("/")[-1] or "photo.jpg"
            files.append(BufferedInputFile(b, filename=name))
            if len(files) >= max_count:
                break

    return files

def _format_caption(item: Dict[str, Any]) -> str:
    caption_lines = []

    title = item.get("title") or item.get("name") or item.get("headline") or "Об'єкт"
    caption_lines.append(f"🏠 {title}")

    addr_str = _format_address(item)
    if addr_str:
        caption_lines.append(f"📍 {addr_str}")

    price = None
    if "price" in item and item["price"]:
        price = item["price"]
    elif "prices" in item and isinstance(item["prices"], dict):
        p = item["prices"].get("value")
        if p:
            try:
                price = f"${int(float(p)):,}".replace(",", " ")
            except Exception:
                price = str(p)

    if price:
        if not isinstance(price, str):
            try:
                price = f"${int(float(price)):,}".replace(",", " ")
            except Exception:
                price = str(price)
        caption_lines.append(f"💵 {price}")

    meta = []
    rooms = item.get("rooms") or item.get("rooms_in") or item.get("roomCount")
    if rooms:
        try:
            meta.append(f"{int(rooms)}к")
        except Exception:
            meta.append(f"{rooms}к")

    area = item.get("area_total") or item.get("area") or item.get("square")
    if area:
        try:
            area_val = float(area)
            if abs(area_val - int(area_val)) < 1e-6:
                area = int(area_val)
        except Exception:
            pass
        meta.append(f"{area} м²")

    if meta:
        caption_lines.append(" · ".join(meta))

    if item.get("id"):
        caption_lines.append(f"ID: {item['id']}")

    import re

    raw_desc_parts: List[str] = []
    for key in ("description", "description_short", "body", "text"):
        v = item.get(key)
        if isinstance(v, str) and v.strip():
            raw_desc_parts.append(v.strip())

    if raw_desc_parts:
        raw_desc = "\n".join(raw_desc_parts)

        raw_desc = re.sub(r'(?i)\b(id[:\-\s]*\d+)\b', '', raw_desc)

        cleaned_lines: List[str] = []
        seen = set()

        for line in raw_desc.splitlines():
            stripped = line.strip()
            if not stripped:
                if cleaned_lines and cleaned_lines[-1] != "":
                    cleaned_lines.append("")
                continue

            norm = re.sub(r"\s+", " ", stripped.lower())
            if norm in seen:
                continue
            seen.add(norm)
            cleaned_lines.append(stripped)

        while cleaned_lines and cleaned_lines[-1] == "":
            cleaned_lines.pop()

        if cleaned_lines:
            base = "\n".join(caption_lines)
            desc = "\n".join(cleaned_lines)
            return f"{base}\n\n{desc}"

    return "\n".join(caption_lines)


async def _show_three_results(message: Message, session: Dict[str, Any]) -> None:
    limit = 3
    offset = int(session.get("page_offset", 0))
    filters = session.get("filters") or {}

    last = session.get("last_query") or {}
    answers = (last.get("answers") or {}) if isinstance(last, dict) else {}
    want_condition = answers.get("condition_in")

    try:
        res = await api.get_apartments(filters, limit=limit, offset=offset)
    except RuntimeError as e:
        if cfg.debug:
            await message.answer(f"(DEBUG) API error: {e}")
        else:
            await message.answer(
                "Виникла помилка на стороні підбору. "
                "Можемо трохи розширити фільтри (район/бюджет) і спробувати ще раз?"
            )
        return

    items = res.get("results") or []
    total = int(res.get("total") or len(items) or 0)

    if not items:
        await _typing(message)
        await message.answer(
            "Поки немає варіантів за цими параметрами. "
            "Можемо розширити район або бюджет — як зручніше?"
        )
        return

    def classify_item_condition(it: Dict[str, Any]) -> Optional[int]:
        for key in ("condition_in", "condition_id", "condition"):
            raw = it.get(key)
            if isinstance(raw, int):
                return raw
            if isinstance(raw, str):
                m = re.search(r"\d+", raw)
                if m:
                    try:
                        return int(m.group(0))
                    except Exception:
                        pass

        parts: List[str] = []
        for key in ("title", "name", "headline",
                    "description", "short_description",
                    "body", "comment", "notes"):
            v = it.get(key)
            if isinstance(v, str) and v.strip():
                parts.append(v.strip())
        if not parts:
            return None
        return _detect_condition_value(" ".join(parts))

    filtered_items: List[Dict[str, Any]] = []
    for it in items:
        if want_condition in (8, 9, 18):
            cond = classify_item_condition(it)
            if cond is not None:
                if want_condition == 8 and cond in (9, 18):
                    continue
                if want_condition in (9, 18) and cond == 8:
                    continue
        filtered_items.append(it)

    used_items: List[Dict[str, Any]] = filtered_items or items

    sent = 0
    for item in used_items[:limit]:
        caption = _format_caption(item)

        text_out = caption

        photos_files = await _fetch_first_n_photos(item, max_count=20)
        if len(photos_files) > 2:
            photos_files = photos_files[::2]
        photos_files = photos_files[:10]

        if len(photos_files) > 1:
            try:
                media = [InputMediaPhoto(media=f) for f in photos_files]
                await message.answer_media_group(media)
                await message.answer(text_out)
            except Exception as e:
                if cfg.debug:
                    await message.answer(
                        f"(DEBUG) media_group error: {e}\n"
                    )
                try:
                    await message.answer_photo(photos_files[0], caption=text_out)
                except Exception as e2:
                    if cfg.debug:
                        await message.answer(
                            f"(DEBUG) photo error: {e2}\n"
                        )
                    await message.answer(text_out)

        elif len(photos_files) == 1:
            try:
                await message.answer_photo(photos_files[0], caption=text_out)
            except Exception as e:
                if cfg.debug:
                    await message.answer(
                        f"(DEBUG) photo error: {e}\n"
                    )
                await message.answer(text_out)
        else:
            await message.answer(text_out)

        sent += 1

    remain = max(0, total - (offset + sent))
    if remain > 0:
        await _typing(message)
        await message.answer(
            f"Є ще приблизно <b>{remain}</b> схожих об’єктів. "
            f"Напишіть «Ще» — пришлю наступні 3 😉"
        )

@dp.message(CommandStart())
async def on_start(message: Message):
    _ensure_loaded()
    await supa.get_or_create_user(message.from_user)
    session = await supa.get_or_create_session(message.from_user.id)

    welcome = WELCOME_TEXT or "Вітаю вас у світі нерухомості без стресу! Я — ШІ-РІЕЛТОР."
    await message.answer(welcome)

    try:
        user = await supa.get_or_create_user_obj(message.from_user.id)
        await supa.append_message(session_id=session["id"], user_uuid=user["id"], direction="out", text=welcome)
    except Exception:
        pass

    ask_name = KEY_TO_TEXT.get("name") or "Як до вас можна звертатись?"
    await _typing(message)
    await message.answer(ask_name)

    await supa.patch_session(session["id"], {
        "status": "active",
        "filters": {},
        "asked_questions": [],
        "missing_questions": _all_questions_except_name(),
        "page_offset": 0,
        "total": 0,
        "contact_received": False,
        "last_query": {"answers": {"name": ""}}
    })

@dp.message(F.content_type == ContentType.CONTACT)
async def on_contact(message: Message):
    _ensure_loaded()
    session = await supa.get_or_create_session(message.from_user.id)
    phone = message.contact.phone_number

    answers = ((session.get("last_query") or {}).get("answers") or {})
    filters = _filters_from_answers(answers)
    session = await supa.patch_session(session["id"], {
        "filters": filters, "page_offset": 0, "contact_received": True
    })

    human = _filters_human(answers, filters)
    pretty = _filters_json_for_sheet(answers, filters)
    full_name = (answers.get("name") or "").strip() or (message.from_user.first_name or "")
    user_row = {
        "full_name": full_name,
        "first_name": message.from_user.first_name,
        "username": message.from_user.username,
        "phone": phone,
        "telegram_user_id": message.from_user.id,
    }

    try:
        append_booking(
            user=user_row,
            listing={"id": "", "title": human},
            filters_human=human,
            filters_json=pretty,
            comment="Initial contact; request filters",
            liked_object_id=""
        )
    except Exception as e:
        if cfg.debug:
            print(f"[Sheets] append_booking failed: {e}")

    await _typing(message)
    await message.answer("Дякую! Надсилаю варіанти 👇", reply_markup=ReplyKeyboardRemove())
    await _show_three_results(message, session)

@dp.message(F.text.regexp(r"^(ще|еще)$", flags=re.I))
async def on_more(message: Message):
    session = await supa.get_or_create_session(message.from_user.id)
    new_offset = int(session.get("page_offset", 0)) + 3
    session = await supa.patch_session(session["id"], {"page_offset": new_offset})
    await _typing(message)
    await _show_three_results(message, session)

_INTENT_VIEW_RE = re.compile(r"(перегляд|показ|на\s+перегляд|зустріч)", re.I)

_INTENT_CONTACT_RE = re.compile(
    r"(зв.?язат(ис|ись)|подзвон(и|іть|ите)?|зателефон\w*|контакт|ріелтор)",
    re.I,
)

_INTENT_LIKE_RE = re.compile(
    r"(сподобал[асьоі]?|сподобавс[яі]|сподобалось|подоба(є|ется)|понравил[асьо])",
    re.I,
)


def _extract_id_from_context(msg: Message) -> Optional[str]:
    if msg.text:
        m = re.search(r"\bID[:\s]*([0-9]{3,})\b", msg.text, re.I)
        if m:
            return m.group(1)
        m = re.search(r"\b([0-9]{4,})\b", msg.text)
        if m:
            return m.group(1)
    if msg.reply_to_message and (msg.reply_to_message.text or msg.reply_to_message.caption):
        src = (msg.reply_to_message.text or msg.reply_to_message.caption)
        m2 = re.search(r"\bID[:\s]*([0-9]{3,})\b", src, re.I)
        if m2:
            return m2.group(1)
    return None

@dp.message(F.text.regexp(_INTENT_VIEW_RE))
async def on_booking(message: Message):
    session = await supa.get_or_create_session(message.from_user.id)
    answers = ((session.get("last_query") or {}).get("answers") or {})
    filters = _filters_from_answers(answers)

    listing_id = _extract_id_from_context(message) or ""
    human = _filters_human(answers, filters)
    pretty = _filters_json_for_sheet(answers, filters)
    full_name = (answers.get("name") or "").strip() or (message.from_user.first_name or "")

    user_row = {
        "full_name": full_name,
        "first_name": message.from_user.first_name,
        "username": message.from_user.username,
        "phone": "",
        "telegram_user_id": message.from_user.id,
    }
    try:
        append_booking(
            user=user_row,
            listing={"id": listing_id, "title": f"Запит на перегляд · {human}"},
            filters_human=human,
            filters_json=pretty,
            comment="Viewing requested",
            liked_object_id=listing_id or ""
        )
    except Exception as e:
        if cfg.debug:
            print(f"[Sheets] append_booking(viewing) failed: {e}")

    await _typing(message)
    await message.answer("Дякую! Наш рієлтор зв’яжеться з вами у будні години (Пн–Пт 09:00–19:00).")


@dp.message(F.text.regexp(_INTENT_LIKE_RE))
async def on_like(message: Message):
    session = await supa.get_or_create_session(message.from_user.id)
    answers = ((session.get("last_query") or {}).get("answers") or {})
    filters = _filters_from_answers(answers)

    listing_id = _extract_id_from_context(message) or ""
    human = _filters_human(answers, filters)
    pretty = _filters_json_for_sheet(answers, filters)
    full_name = (answers.get("name") or "").strip() or (message.from_user.first_name or "")

    if not listing_id:
        await _typing(message)
        await message.answer(
            "Бачу, що вам сподобався варіант 😊\n"
            "Щоб я міг передати його рієлтору, напишіть, будь ласка, "
            "ID об’єкта (наприклад: <b>ID 21364</b>) або відповідайте "
            "«сподобалась» прямо на повідомлення з оголошенням."
        )
        return

    user_row = {
        "full_name": full_name,
        "first_name": message.from_user.first_name,
        "username": message.from_user.username,
        "phone": "",
        "telegram_user_id": message.from_user.id,
    }

    try:
        append_booking(
            user=user_row,
            listing={"id": listing_id, "title": f"Сподобався об’єкт · {human}"},
            filters_human=human,
            filters_json=pretty,
            comment="Liked listing",
            liked_object_id=listing_id,
        )
    except Exception as e:
        if cfg.debug:
            print(f"[Sheets] append_booking(like) failed: {e}")

    await _typing(message)
    await message.answer(
        f"Зафіксував, що вам сподобався об’єкт з ID <b>{listing_id}</b>.\n"
        "Рієлтор врахує це при подальшому підборі 👍"
    )


@dp.message(F.text.regexp(_INTENT_CONTACT_RE))
async def on_contact_request(message: Message):
    session = await supa.get_or_create_session(message.from_user.id)
    answers = ((session.get("last_query") or {}).get("answers") or {})
    filters = _filters_from_answers(answers)
    human = _filters_human(answers, filters)
    pretty = _filters_json_for_sheet(answers, filters)
    full_name = (answers.get("name") or "").strip() or (message.from_user.first_name or "")

    user_row = {
        "full_name": full_name,
        "first_name": message.from_user.first_name,
        "username": message.from_user.username,
        "phone": "",
        "telegram_user_id": message.from_user.id,
    }
    try:
        append_booking(
            user=user_row,
            listing={"id": "", "title": f"Запит на контакт · {human}"},
            filters_human=human,
            filters_json=pretty,
            comment="Contact requested",
            liked_object_id=""
        )
    except Exception as e:
        if cfg.debug:
            print(f"[Sheets] append_booking(contact) failed: {e}")

    await _typing(message)
    await message.answer("Передав контакт рієлтору. Він відповість у робочий час (Пн–Пт 09:00–19:00).")

@dp.message(
    F.text
    & ~F.text.regexp(_INTENT_VIEW_RE)
    & ~F.text.regexp(_INTENT_CONTACT_RE)
    & ~F.text.regexp(_INTENT_LIKE_RE)
)
async def on_text(message: Message):
    _ensure_loaded()
    text_in = (message.text or "").strip()

    if text_in.lower() in {"ще", "еще"}:
        return

    session = await supa.get_or_create_session(message.from_user.id)
    old_filters = session.get("filters") or {}

    try:
        user = await supa.get_or_create_user_obj(message.from_user.id)
        await supa.append_message(
            session_id=session["id"],
            user_uuid=user["id"],
            direction="in",
            text=text_in
        )
    except Exception:
        pass

    last = dict(session.get("last_query") or {})
    answers = dict(last.get("answers") or {})

    if not answers.get("name"):
        answers["name"] = text_in
        last["answers"] = answers
        await supa.patch_session(session["id"], {"last_query": last})

        await _typing(message)
        await message.answer(WELCOME_AFTER_NAME.format(name=answers["name"]))
        await message.answer(_bulleted(_all_questions_except_name()))
        return

    answers = _parse_into_answers(text_in, answers, old_filters=old_filters)
    last["answers"] = answers
    missing = _missing_now(answers)

    contact_received = bool(session.get("contact_received"))

    if contact_received:
        filters = _filters_from_answers(answers)
        diff_str = _filters_diff_human(old_filters, filters)

        session = await supa.patch_session(session["id"], {
            "last_query": last,
            "filters": filters,
            "page_offset": 0,
            "missing_questions": missing,
        })

        await _typing(message)
        if diff_str:
            await message.answer(f"Зрозумів, оновив підбір: {diff_str} 👇")
        else:
            await message.answer("Оновив підбір за вашими побажаннями 👇")

        await _show_three_results(message, session)
        return

    await supa.patch_session(session["id"], {
        "asked_questions": session.get("asked_questions") or [],
        "last_query": last,
        "missing_questions": missing,
    })

    if not missing:
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📞 Поділитись контактом", request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await _typing(message)
        await message.answer(
            "Усе запам'ятав. Готовий приступити до пошуку 👇 Поділіться, будь ласка, номером телефону.",
            reply_markup=kb,
        )
        try:
            user = await supa.get_or_create_user_obj(message.from_user.id)
            await supa.append_message(
                session_id=session["id"],
                user_uuid=user["id"],
                direction="out",
                text="ask_contact",
            )
        except Exception:
            pass
        return

    await _typing(message)
    await message.answer(_bulleted(missing))


async def main():
    try:
        await dp.start_polling(bot)
    finally:
        try:
            if _http_session and not _http_session.closed:
                await _http_session.close()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())
