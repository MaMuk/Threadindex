from __future__ import annotations

import hashlib
import json
from typing import Any


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def canonical_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def fingerprint_conversation(
    title: str,
    created_at: int,
    updated_at: int,
    messages: list[dict[str, Any]],
) -> str:
    payload = {
        "title": title or "",
        "created_at": created_at,
        "updated_at": updated_at,
        "messages": messages,
    }
    return sha256_text(canonical_json(payload))


def fingerprint_message(
    conversation_id: str,
    author_role: str,
    content: str,
    created_at: int,
    sequence: int,
) -> str:
    payload = {
        "conversation_id": conversation_id,
        "author_role": author_role,
        "content": content,
        "created_at": created_at,
        "sequence": sequence,
    }
    return sha256_text(canonical_json(payload))
