from __future__ import annotations
from typing import Dict, List

WELCOME_AFTER_NAME = "Дуже приємно познайомитись, {name}. Щоб бути максимально корисним для вас, я задам декілька запитань."

def format_questions_bulleted(questions: List[Dict]) -> str:
    lines = ["Чудово, майже все є! Ще підкажіть, будь ласка:"]
    for q in questions:
        lines.append(f"• {q['question_text']}")
    return "\n".join(lines)

def detect_missing(answers: Dict[str, str], ordered_keys: List[str]) -> List[str]:
    missing: List[str] = []
    for k in ordered_keys:
        v = (answers or {}).get(k)
        if v is None or str(v).strip() == "":
            missing.append(k)
    return missing
