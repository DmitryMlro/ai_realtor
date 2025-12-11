from __future__ import annotations
import asyncio
import uuid
import time
from typing import Any, Dict, Optional

try:
    from supabase import create_client, Client as SupabaseClientSDK  # type: ignore
except Exception:
    create_client = None
    SupabaseClientSDK = object  # stub

from config import cfg


class SupabaseClient:

    def __init__(self):
        self.url: str = cfg.supabase_url
        self.key: str = getattr(cfg, "supabase_key", "") or getattr(cfg, "supabase_anon_key", "")
        self.enabled: bool = bool(self.url and self.key and create_client is not None)

        self._mem_users: Dict[int, Dict[str, Any]] = {}
        self._mem_sessions: Dict[int, Dict[str, Any]] = {}
        self._mem_messages: Dict[str, list] = {}

        if self.enabled:
            try:
                self.client: SupabaseClientSDK = create_client(self.url, self.key)
            except Exception:
                self.enabled = False

    def _mem_user_id(self, telegram_user_id: int) -> str:
        return f"user_{telegram_user_id}"

    def _mem_session_id(self, telegram_user_id: int) -> str:
        return f"session_{telegram_user_id}"

    async def get_or_create_user(self, tg_user) -> Dict[str, Any]:
        if not tg_user:
            raise ValueError("tg_user is required")

        if self.enabled:
            try:
                data = {
                    "telegram_user_id": tg_user.id,
                    "username": tg_user.username,
                    "first_name": tg_user.first_name,
                }
                res = self.client.table("users").upsert(data, on_conflict="telegram_user_id").execute()
                if res.data:
                    return res.data[0]
            except Exception:
                pass
        u = self._mem_users.get(tg_user.id) or {
            "id": str(uuid.uuid4()),
            "telegram_user_id": tg_user.id,
            "username": tg_user.username,
            "first_name": tg_user.first_name,
            "created_at": int(time.time()),
        }
        self._mem_users[tg_user.id] = u
        return u

    async def get_or_create_user_obj(self, telegram_user_id: int) -> Dict[str, Any]:
        if self.enabled:
            try:
                res = self.client.table("users").select("*").eq("telegram_user_id", telegram_user_id).limit(1).execute()
                if res.data:
                    return res.data[0]
            except Exception:
                pass
        u = self._mem_users.get(telegram_user_id)
        if not u:
            u = {
                "id": str(uuid.uuid4()),
                "telegram_user_id": telegram_user_id,
                "username": None,
                "first_name": None,
                "created_at": int(time.time()),
            }
            self._mem_users[telegram_user_id] = u
        return u

    async def get_or_create_session(self, telegram_user_id: int) -> Dict[str, Any]:
        if self.enabled:
            try:
                res = self.client.table("sessions").select("*").eq("telegram_user_id", telegram_user_id).eq("status",
                                                                                                            "active").limit(
                    1).execute()
                if res.data:
                    return res.data[0]
                data = {
                    "telegram_user_id": telegram_user_id,
                    "status": "active",
                    "created_at": int(time.time()),
                    "last_query": {"answers": {}},
                    "filters": {},
                    "asked_questions": [],
                    "missing_questions": [],
                    "page_offset": 0,
                    "total": 0,
                }
                res = self.client.table("sessions").insert(data).execute()
                return res.data[0]
            except Exception:
                pass

        sid = self._mem_session_id(telegram_user_id)
        s = self._mem_sessions.get(telegram_user_id)
        if not s:
            s = {
                "id": sid,
                "telegram_user_id": telegram_user_id,
                "status": "active",
                "created_at": int(time.time()),
                "last_query": {"answers": {}},
                "filters": {},
                "asked_questions": [],
                "missing_questions": [],
                "page_offset": 0,
                "total": 0,
            }
            self._mem_sessions[telegram_user_id] = s
        return s

    async def patch_session(self, session_id_or_obj: Any, patch: Dict[str, Any]) -> Dict[str, Any]:
        if self.enabled:
            try:
                if isinstance(session_id_or_obj, dict):
                    sid = session_id_or_obj.get("id")
                else:
                    sid = session_id_or_obj
                res = self.client.table("sessions").update(patch).eq("id", sid).execute()
                if res.data:
                    return res.data[0]
            except Exception:
                pass

        if isinstance(session_id_or_obj, dict):
            tuid = session_id_or_obj.get("telegram_user_id")
            if tuid and tuid in self._mem_sessions:
                self._mem_sessions[tuid].update(patch)
                return self._mem_sessions[tuid]
        for tuid, s in self._mem_sessions.items():
            if s.get("id") == session_id_or_obj:
                s.update(patch)
                return s
        return session_id_or_obj if isinstance(session_id_or_obj, dict) else {"id": session_id_or_obj, **patch}

    async def append_message(self, session_id: Any, user_uuid: Any, direction: str, text: str) -> None:
        if self.enabled:
            try:
                data = {
                    "session_id": session_id,
                    "user_uuid": user_uuid,
                    "direction": direction,
                    "text": text,
                    "ts": int(time.time()),
                }
                self.client.table("messages").insert(data).execute()
                return
            except Exception:
                pass

        sid = str(session_id)
        self._mem_messages.setdefault(sid, []).append({
            "user_uuid": user_uuid,
            "direction": direction,
            "text": text,
            "ts": int(time.time()),
        })
