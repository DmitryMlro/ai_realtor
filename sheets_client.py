import json
import datetime as dt
from typing import Any, Dict, List, Optional

import gspread
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

from config import cfg

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

DEFAULT_BOOKINGS_HEADER = [
    "timestamp",
    "full_name",
    "phone",
    "tg_username",
    "liked_object_id",
    "liked_summary",
    "filters_json",
    "comment",
    "telegram_user_id",
    "listing_id",
    "listing_title",
    "filters_human",
]


class SheetsClient:

    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self._gc = self._auth()
        self._sh = self._gc.open_by_key(self.spreadsheet_id)

    def _auth(self):
        path = getattr(cfg, "gs_service_account_json_path", None) or getattr(
            cfg, "GS_SERVICE_ACCOUNT_JSON_PATH", None
        )
        if not path:
            path = "credentials/service_account.json"
        creds = Credentials.from_service_account_file(path, scopes=SCOPES)
        return gspread.authorize(creds)

    def _find_ws_ci(self, name: str) -> Optional[gspread.Worksheet]:
        target = name.strip().lower()
        try:
            for ws in self._sh.worksheets():
                if ws.title.strip().lower() == target:
                    return ws
        except Exception:
            pass
        return None

    def _get_or_create_bookings_ws(self) -> gspread.Worksheet:
        ws = self._find_ws_ci("Bookings")
        if ws:
            return ws
        ws = self._sh.add_worksheet(title="Bookings", rows=2000, cols=20)
        try:
            ws.append_row(DEFAULT_BOOKINGS_HEADER, value_input_option="RAW")
        except Exception:
            pass
        return ws

    @staticmethod
    def _normalize_header(header: List[str]) -> List[str]:
        return [str(h or "").strip() for h in header]

    def _ensure_bookings_header(self, ws: gspread.Worksheet) -> List[str]:
        header = ws.row_values(1)
        if not header:
            ws.update("A1", [DEFAULT_BOOKINGS_HEADER])
            header = DEFAULT_BOOKINGS_HEADER[:]
        return self._normalize_header(header)

    def _find_existing_row_index(
            self,
            ws: gspread.Worksheet,
            header: List[str],
            user: Dict[str, Any],
    ) -> Optional[int]:
        header_lc = [h.lower() for h in header]

        key_col_name = None
        key_value = None

        if "telegram_user_id" in header_lc:
            key_col_name = "telegram_user_id"
            key_value = str(user.get("telegram_user_id") or "").strip()
        elif "phone" in header_lc:
            key_col_name = "phone"
            key_value = str(user.get("phone") or "").strip()

        if not key_col_name or not key_value:
            return None

        col_idx = header_lc.index(key_col_name) + 1

        try:
            col_values = ws.col_values(col_idx)
        except Exception as e:
            if getattr(cfg, "debug", False):
                print(f"[Sheets] col_values error: {e}")
            return None

        for i, val in enumerate(col_values[1:], start=2):
            if str(val).strip() == key_value:
                return i

        return None

    @staticmethod
    def _now_str() -> str:
        return dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    def _build_row_by_header(
            self,
            header: List[str],
            user: Dict[str, Any],
            listing: Dict[str, Any],
            filters_human: str,
            filters_json: Dict[str, Any],
            comment: str,
            liked_object_id: Optional[str],
            liked_summary: Optional[str],
            existing_row_map: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        existing_row_map = existing_row_map or {}

        base: Dict[str, Any] = dict(existing_row_map)

        base["timestamp"] = self._now_str()
        base["full_name"] = user.get("full_name") or base.get("full_name") or ""
        base["phone"] = user.get("phone") or base.get("phone") or ""
        base["tg_username"] = user.get("username") or base.get("tg_username") or ""
        base["telegram_user_id"] = user.get("telegram_user_id") or base.get("telegram_user_id") or ""

        base["listing_id"] = listing.get("id") or base.get("listing_id") or ""
        base["listing_title"] = listing.get("title") or base.get("listing_title") or ""

        base["filters_human"] = filters_human or base.get("filters_human") or ""

        base["filters_json"] = json.dumps(filters_json or {}, ensure_ascii=False)

        if liked_object_id is not None:
            base["liked_object_id"] = liked_object_id
        else:
            base["liked_object_id"] = base.get("liked_object_id") or base.get("listing_id") or ""

        base["liked_summary"] = liked_summary or filters_human or base.get("liked_summary") or ""

        if comment:
            base["comment"] = comment
        else:
            base["comment"] = base.get("comment") or ""

        return [base.get(col, "") for col in header]

    def get_welcome(self, lang: str = "ukrainian") -> Optional[str]:
        ws = self._find_ws_ci("welcome_messages")
        if not ws:
            return None
        try:
            rows = ws.get_all_records()
            for r in rows:
                if (
                        str(r.get("key", "")).strip().lower() == "welcome"
                        and str(r.get("lang", "")).strip().lower() == lang.lower()
                ):
                    return r.get("text") or None
        except Exception as e:
            if getattr(cfg, "debug", False):
                print(f"[Sheets] get_welcome() error: {e}")
        return None

    def get_questions(self) -> List[Dict[str, Any]]:
        ws = self._find_ws_ci("questions")
        if not ws:
            return []
        try:
            rows = ws.get_all_records()
            rows = [r for r in rows if r.get("question_key") and r.get("question_text")]

            def ord_key(r):
                try:
                    return int(r.get("order"))
                except Exception:
                    return 10_000

            rows.sort(key=ord_key)
            return rows
        except Exception as e:
            if getattr(cfg, "debug", False):
                print(f"[Sheets] get_questions() error: {e}")
            return []

    def append_booking(
            self,
            user: Dict[str, Any],
            listing: Dict[str, Any],
            filters_human: str,
            filters_json: Dict[str, Any],
            comment: str = "",
            liked_object_id: Optional[str] = None,
            liked_summary: Optional[str] = None,
    ) -> None:
        ws = self._get_or_create_bookings_ws()
        header = self._ensure_bookings_header(ws)

        existing_row_idx: Optional[int] = self._find_existing_row_index(ws, header, user)
        existing_row_map: Dict[str, Any] = {}

        if existing_row_idx is not None:
            try:
                existing_values = ws.row_values(existing_row_idx)
                for i, col in enumerate(header):
                    if i < len(existing_values):
                        existing_row_map[col] = existing_values[i]
            except Exception as e:
                if getattr(cfg, "debug", False):
                    print(f"[Sheets] read existing row error: {e}")

        row = self._build_row_by_header(
            header,
            user,
            listing,
            filters_human,
            filters_json,
            comment,
            liked_object_id,
            liked_summary,
            existing_row_map=existing_row_map,
        )

        try:
            if existing_row_idx is None:
                ws.append_row(row, value_input_option="USER_ENTERED")
            else:
                last_a1 = rowcol_to_a1(1, len(header))  # напр. 'K1'
                last_col_letter = "".join(ch for ch in last_a1 if ch.isalpha())
                rng = f"A{existing_row_idx}:{last_col_letter}{existing_row_idx}"
                ws.update(rng, [row], value_input_option="USER_ENTERED")
        except Exception as e:
            if getattr(cfg, "debug", False):
                print(f"[Sheets] append/update booking error: {e}")


_sheets_singleton: Optional[SheetsClient] = None


def _get_singleton() -> SheetsClient:
    global _sheets_singleton
    if _sheets_singleton is None:
        _sheets_singleton = SheetsClient(cfg.sheets_id)
    return _sheets_singleton


def append_booking(
        user: Dict[str, Any],
        listing: Dict[str, Any],
        filters_human: str,
        filters_json: Dict[str, Any],
        comment: str = "",
        liked_object_id: Optional[str] = None,
        liked_summary: Optional[str] = None,
) -> None:
    client = _get_singleton()
    client.append_booking(
        user,
        listing,
        filters_human,
        filters_json,
        comment,
        liked_object_id,
        liked_summary,
    )
