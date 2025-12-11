import re
import unicodedata
from typing import Dict, Any, Optional, Tuple

import json
from pathlib import Path


def _norm_simple(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = unicodedata.normalize("NFKD", s)
    s = s.replace("’", "'").replace("`", "'").replace("ʼ", "'")
    s = re.sub(r"[^0-9a-zа-яёєіїґ\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _load_location_keywords() -> dict:
    try:
        path = Path(__file__).with_name("location_keywords.json")
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


LOCATION_KEYWORDS = _load_location_keywords()


def _detect_location_from_keywords(text: str) -> Optional[str]:
    if not text or not LOCATION_KEYWORDS:
        return None

    t_norm = _norm_simple(text)

    best_match = None
    best_len = 0

    for norm_key, original in LOCATION_KEYWORDS.items():
        if norm_key in t_norm and len(norm_key) > best_len:
            best_match = original
            best_len = len(norm_key)

    return best_match


def _match_label_id(text_norm: str, labels: Dict[int, str]) -> Optional[int]:
    best_id = None
    best_len = 0

    for lid, name in labels.items():
        if not isinstance(name, str) or not name.strip():
            continue
        ln = _norm_simple(name)
        if len(ln) < 3:
            continue
        if ln in text_norm:
            if len(ln) > best_len:
                best_len = len(ln)
                best_id = lid

    return best_id


def _detect_location(text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not text:
        return out

    t_norm = _norm_simple(text)

    mid = _detect_microarea(text)
    if mid is not None:
        out["microarea_id"] = mid
        out["district_text"] = MICROAREA_LABELS.get(mid)
        return out

    did = _detect_district(text)
    if did is not None:
        out["district_id"] = did
        out["district_text"] = DISTRICT_LABELS.get(did)
        return out

    loc_from_dict = _detect_location_from_keywords(text)
    if loc_from_dict:
        out["district_text"] = loc_from_dict
        return out

    if re.search(r"та[иі]ров", t_norm):
        out["district_text"] = "Таїрова"

    if re.search(r"центр(и|е|а)?", t_norm):
        out["district_text"] = "Центр"

    if re.search(r"фонтан", t_norm):
        out["district_text"] = "Фонтан"

    return out


def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = unicodedata.normalize("NFKD", s)
    s = s.replace("’", "'").replace("`", "'").replace("ʼ", "'")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


DISTRICT_LABELS = {
    5: "Київський",
    6: "Малиновський",
    8: "Приморський",
    11: "Суворовський",
}

MICROAREA_LABELS = {
    89: "Бугаївка",
    90: "пос. Дзержинського",
    91: "Застава",
    92: "Ленпоселок",
    93: "Мельниці",
    94: "Молдаванка",
    95: "пос. Сахарний",
    96: "Слободка",
    97: "Фонтан",
    98: "Черемушки",
    99: "Аркадія",
    102: "Центр",
    103: "Шевченко-Французький (Французький бульвар)",
    104: "Большевик",
    105: "пос. Котовського",
    106: "Крива Балка",
    107: "Куяльник",
    108: "Лузановка",
    109: "пос. Нафтовиків",
    110: "Пересип",
    112: "Шевченко",
    113: "Вузівський",
    114: "Дача Ковалевського",
    115: "Дружний",
    116: "Таїрова",
    118: "Царське село",
    119: "Червоний Хутір",
    121: "Чорноморка",
    122: "Чубаївка",
}

DISTRICTS = {
    5: ["київський", "киевский", "київському", "киевском", "київськ"],
    6: ["малиновський", "малиновский", "малиновському", "малиновск"],
    8: ["приморський", "приморский", "приморському", "приморск"],
    11: ["суворовський", "суворовский", "суворовському", "суворовск"],
}

MICROAREAS = {
    89: ["бугaївка", "бугаевка", "бугaївці", "бугаевке", "бугаївка"],
    90: ["дзержинського", "дзержинского", "пос дзержинського", "пос. дзержинского"],
    91: ["застава", "заставі", "заставе"],
    92: ["ленпоселок", "ленпоселку"],
    93: ["мельниці", "мельницы"],
    94: ["молдаванка", "молдаванці", "молдаванке"],
    95: ["сахарний", "сахарный", "пос сахарный", "пос. сахарный"],
    96: ["слободка", "слободці", "слободке"],
    97: ["фонтан", "великий фонтан", "фонтані", "фонтане"],
    98: ["черемушки", "черомушки", "черемушкі"],
    99: ["аркадія", "аркадия", "аркадії", "аркадии"],
    102: ["центр", "центрі", "центре", "центральний"],
    103: ["шевченко-французький", "шевченко французький", "французький бульвар", "французский бульвар"],
    104: ["большевик", "більшовик"],
    105: ["котовського", "пос котовского", "пос. котовского", "котовского"],
    106: ["крива балка", "кривая балка"],
    107: ["куяльник", "куяльнику", "куяльнике"],
    108: ["лузановка", "лузановці", "лузановке"],
    109: ["нафтовиків", "нефтяников", "пос нефтяников", "пос. нефтяников"],
    110: ["пересип", "пересыпь"],
    112: ["шевченко"],
    113: ["вузівський", "вузовский"],
    114: ["дача ковалевского", "дача ковалевського"],
    115: ["дружний", "дружний ж/м", "дружный"],
    116: ["таїрова", "таирова", "таїрово", "таирово"],
    118: ["царське село", "царское село"],
    119: ["червоний хутір", "червоный хутор", "красный хутор"],
    121: ["чорноморка", "черноморка"],
    122: ["чубаївка", "чубаевка"],
}


def _lev1(a: str, b: str) -> bool:
    if a == b:
        return True
    if abs(len(a) - len(b)) > 1:
        return False
    i = j = diff = 0
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            i += 1;
            j += 1
        else:
            diff += 1
            if diff > 1:
                return False
            if len(a) > len(b):
                i += 1
            elif len(b) > len(a):
                j += 1
            else:
                i += 1;
                j += 1
    diff += (len(a) - i) + (len(b) - j)
    return diff <= 1


def _fuzzy_contains(t: str, variant: str) -> bool:
    t = _norm(t)
    v = _norm(variant)
    for w in t.split():
        if _lev1(w, v):
            return True
    return re.search(rf"\b{re.escape(v)}\b", t) is not None


def _detect_district(text: str) -> Optional[int]:
    for did, variants in DISTRICTS.items():
        for v in variants:
            if _fuzzy_contains(text, v):
                return did
    return None


def _detect_microarea(text: str) -> Optional[int]:
    for mid, variants in MICROAREAS.items():
        for v in variants:
            if _fuzzy_contains(text, v):
                return mid
    return None


ROOMS_NUMBER_MAP = {
    "однокімнат": 1, "одна кімната": 1, "1к": 1, "1 к": 1, "1 комнатна": 1,  "1 кімнатна": 1,
    "двокімнат": 2, "дві кімнати": 2, "двухкомнат": 2, "2к": 2, "2 к": 2, "2 комнатна": 2, "2 кімнатна": 2,
    "трикімнат": 3, "три кімнати": 3, "трехкомнат": 3, "3к": 3, "3 к": 3, "3 комнатна": 3, "3 кімнатна": 3,
    "чотирикімнат": 4, "чотири кімнати": 4, "четырехкомнат": 4, "4к": 4, "4 к": 4, "4 комнатна": 4, "4 кімнатна": 4,
}

_ROOMS_RE = re.compile(
    flags=re.IGNORECASE | re.VERBOSE,
)


def _parse_rooms(text: str) -> Optional[int]:
    t = _norm_simple(text)
    if not t:
        return None

    ROOM_PATTERNS = {
        1: [
            r"однуш\w*",
            r"однокiмн\w*",
            r"однокімнат\w*",
            r"однокомнат\w*",
        ],
        2: [
            r"двуш\w*",
            r"двокiмн\w*",
            r"двокімнат\w*",
            r"двухкомнат\w*",
        ],
        3: [
            r"трешк\w*",
            r"трішк\w*",
            r"тришк\w*",
            r"трикімнат\w*",
            r"трехкомнат\w*",
        ],
        4: [
            r"четырехкомнат\w*",
            r"чотирикiмн\w*",
            r"чотирикімнат\w*",
        ],
    }

    for rooms, patterns in ROOM_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, t):
                return rooms

    m = re.search(
        r"\b([1-4])\s*(кімнат\w*|кімн\w*|комнат\w*|комн\w*)",
        t,
    )
    if m:
        return int(m.group(1))

    m2 = re.search(r"\b([1-4])\s*к\b", t)
    if m2:
        return int(m2.group(1))

    return None



_BUDGET_RE = re.compile(
    r"""
    (?:до\s*)?
    ([$€₴]?\s*\d[\d\s]*[kк]?)
    \s*
    (usd|дол|долар|доларів|\$|€|eur|євро|грн|uah|гривень|гривні)?
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)


def _parse_budget_value(text: str) -> Optional[int]:
    t = _norm_simple(text)
    if not t:
        return None

    candidates = []

    for m in _BUDGET_RE.finditer(t):
        raw = m.group(1) or ""
        cur = (m.group(2) or "").lower()

        cleaned = (
            raw.replace("$", "")
            .replace("€", "")
            .replace("₴", "")
            .replace(" ", "")
        )

        if not cur and re.fullmatch(r"[1-9][kк]", cleaned):
            continue

        if cleaned.endswith(("k", "к")):
            num = cleaned[:-1]
            if num.isdigit():
                candidates.append(int(num) * 1000)
            continue

        if cleaned.isdigit():
            val = int(cleaned)
            if val < 10:
                continue
            candidates.append(val)

    if not candidates:
        return None

    return max(candidates)


def _parse_condition(text: str) -> Optional[int]:
    t = _norm_simple(text)
    if not t:
        return None

    if (
        "евроремонт" in t
        or "євроремонт" in t
        or "готовим ремонтом" in t
        or re.search(r"(з|с)\s+ремонт", t)
    ):
        return 8

    if "під ремонт" in t or "под ремонт" in t or "требує ремонт" in t or "требует ремонт" in t:
        return 18

    if (
        "без ремонт" in t
        or "от строител" in t
        or "после строител" in t
        or "от застройщик" in t or "застройщика" in t
        or "вид будивельник" in t or "вид будівельник" in t
        or "від будівельник" in t
        or "від забудовник" in t or "вид забудовник" in t
        or "пiсля будiвельник" in t
        or "після будівельник" in t
        or "в новобудов" in t
        or "в новостройк" in t
    ):
        return 9

    if "під оздоб" in t or "под отделоч" in t:
        return 6

    if "капитальн" in t or "капітальн" in t or "хороший ремонт" in t:
        return 14

    return None


def interpret_answer_for_key(key: str, text: str):
    if key == "rooms_in":
        return _parse_rooms(text)
    if key in ("budget", "price_max"):
        return _parse_budget_value(text)
    if key == "condition_in":
        return _parse_condition(text)
    if key == "district_id":
        return _detect_district(text)
    if key == "microarea_id":
        return _detect_microarea(text)
    if key == "type":
        t = _norm(text)
        if "квартир" in t:
            return "квартира"
        if "будин" in t or "котедж" in t:
            return "будинок"
        return None
    return None


def parse_free_text(text: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not text:
        return result

    t_norm = _norm_simple(text)

    if "квартир" in t_norm or "kvartir" in t_norm:
        result["type"] = "apartment"
    elif "будинок" in t_norm or "будин" in t_norm or "дом" in t_norm or "house" in t_norm:
        result["type"] = "house"

    rooms = _parse_rooms(text)
    if rooms is not None:
        result["rooms_in"] = rooms

    budget = _parse_budget_value(text)
    if budget is not None:
        result["price_max"] = budget

    cond = _parse_condition(text)
    if cond is not None:
        result["condition_in"] = cond

    loc = _detect_location(text)
    result.update(loc)

    return result


