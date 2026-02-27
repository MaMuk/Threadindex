from __future__ import annotations

from typing import Any

from ..models import ConversationData, MessageData
from ..utils import fingerprint_conversation, fingerprint_message, safe_int


class ChatGPTConversationImporter:
    source_type = "chatgpt"

    def extract_conversations(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if self._is_chatgpt_conversation(item)]

        if not isinstance(payload, dict):
            return []

        if self._is_chatgpt_conversation(payload):
            return [payload]

        conversations = payload.get("conversations")
        if isinstance(conversations, list):
            return [item for item in conversations if self._is_chatgpt_conversation(item)]
        if isinstance(conversations, dict):
            items = conversations.get("items")
            if isinstance(items, list):
                return [item for item in items if self._is_chatgpt_conversation(item)]
            data = conversations.get("data")
            if isinstance(data, list):
                return [item for item in data if self._is_chatgpt_conversation(item)]

        items = payload.get("items")
        if isinstance(items, list):
            return [item for item in items if self._is_chatgpt_conversation(item)]

        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if self._is_chatgpt_conversation(item)]

        return []

    def normalize_conversation(self, raw: dict[str, Any], file_source: str) -> ConversationData:
        title = (raw.get("title") or "(untitled)").strip()
        created_at = safe_int(raw.get("create_time"))
        updated_at = safe_int(raw.get("update_time"))

        mapping = raw.get("mapping") or {}
        messages: list[dict[str, Any]] = []
        for node in mapping.values():
            message = node.get("message") if isinstance(node, dict) else None
            if not message:
                continue
            author_role = (
                (message.get("author") or {}).get("role")
                if isinstance(message.get("author"), dict)
                else "unknown"
            )
            content_text = self._extract_content(message)
            if content_text is None:
                continue
            msg_id = message.get("id") or node.get("id")
            msg_created = safe_int(message.get("create_time"))
            messages.append(
                {
                    "id": msg_id,
                    "author_role": author_role or "unknown",
                    "content": content_text,
                    "created_at": msg_created,
                }
            )

        messages.sort(key=lambda m: (m["created_at"], m["id"] or ""))

        message_payloads = [
            {
                "author_role": msg["author_role"],
                "content": msg["content"],
                "created_at": msg["created_at"],
            }
            for msg in messages
        ]

        content_hash = fingerprint_conversation(title, created_at, updated_at, message_payloads)
        conversation_id = raw.get("id") or content_hash

        message_data: list[MessageData] = []
        for idx, msg in enumerate(messages):
            sequence = idx + 1
            message_hash = fingerprint_message(
                conversation_id,
                msg["author_role"],
                msg["content"],
                msg["created_at"],
                sequence,
            )
            message_id = msg["id"] or f"msg_{message_hash}"
            message_data.append(
                MessageData(
                    id=message_id,
                    conversation_id=conversation_id,
                    author_role=msg["author_role"],
                    content=msg["content"],
                    created_at=msg["created_at"],
                    content_hash=message_hash,
                    sequence=sequence,
                )
            )

        return ConversationData(
            id=conversation_id,
            title=title,
            created_at=created_at,
            updated_at=updated_at,
            source=self.source_type,
            file_source=file_source,
            content_hash=content_hash,
            message_count=len(message_data),
            messages=message_data,
        )

    @staticmethod
    def _is_chatgpt_conversation(data: Any) -> bool:
        if not isinstance(data, dict):
            return False
        mapping = data.get("mapping")
        if not isinstance(mapping, dict):
            return False

        # ChatGPT exports usually carry author role + content structure on mapping messages.
        for node in mapping.values():
            if not isinstance(node, dict):
                continue
            message = node.get("message")
            if not isinstance(message, dict):
                continue
            author = message.get("author")
            if not isinstance(author, dict):
                continue
            role = author.get("role")
            if isinstance(role, str) and role:
                return True
        return False

    @staticmethod
    def _extract_content(message: dict[str, Any]) -> str | None:
        content = message.get("content")
        if content is None:
            return None
        if isinstance(content, str):
            return content
        if isinstance(content, dict):
            parts = content.get("parts")
            if isinstance(parts, list):
                return "\n".join(str(part) for part in parts if part is not None)
            if "text" in content and isinstance(content["text"], str):
                return content["text"]
            if "result" in content and isinstance(content["result"], str):
                return content["result"]
        return None
