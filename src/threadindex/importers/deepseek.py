from __future__ import annotations

from datetime import datetime
from typing import Any

from ..models import ConversationData, MessageData
from ..utils import fingerprint_conversation, fingerprint_message, safe_int


class DeepseekConversationImporter:
    source_type = "deepseek"

    def extract_conversations(self, payload: Any) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        seen_ids: set[int] = set()

        def walk(node: Any) -> None:
            node_id = id(node)
            if node_id in seen_ids:
                return
            seen_ids.add(node_id)

            if isinstance(node, dict):
                if self._is_deepseek_conversation(node):
                    results.append(node)
                    return
                for value in node.values():
                    if isinstance(value, (dict, list)):
                        walk(value)
                return

            if isinstance(node, list):
                for item in node:
                    if isinstance(item, (dict, list)):
                        walk(item)

        walk(payload)
        return results

    def normalize_conversation(self, raw: dict[str, Any], file_source: str) -> ConversationData:
        title = (
            raw.get("title") or raw.get("name") or raw.get("chat_name") or "(untitled)"
        ).strip()
        created_at = self._pick_timestamp(
            raw, "create_time", "created_at", "createdAt", "created", "inserted_at"
        )
        updated_at = self._pick_timestamp(
            raw, "update_time", "updated_at", "updatedAt", "updated", "inserted_at"
        )

        raw_messages = self._extract_messages(raw)
        messages: list[dict[str, Any]] = []
        for msg in raw_messages:
            if not isinstance(msg, dict):
                continue
            role = self._extract_role(msg)
            content = self._extract_content(msg)
            if content is None:
                continue
            msg_id = msg.get("id") or msg.get("message_id")
            msg_created = self._pick_timestamp(
                msg, "create_time", "created_at", "createdAt", "time", "timestamp", "inserted_at"
            )
            messages.append(
                {
                    "id": msg_id,
                    "author_role": role,
                    "content": content,
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
        conversation_id = (
            raw.get("id")
            or raw.get("conversation_id")
            or raw.get("chat_id")
            or raw.get("session_id")
            or content_hash
        )

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
            raw_message_id = msg["id"]
            message_id = (
                f"{conversation_id}:{raw_message_id}" if raw_message_id else f"msg_{message_hash}"
            )
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

    def _is_deepseek_conversation(self, data: Any) -> bool:
        if not isinstance(data, dict):
            return False
        mapping = data.get("mapping")
        if isinstance(mapping, dict) and any(
            isinstance(node, dict) and isinstance(node.get("message"), dict)
            for node in mapping.values()
        ):
            return True
        messages = self._extract_messages(data)
        if not isinstance(messages, list) or not messages:
            return False
        for message in messages:
            if not isinstance(message, dict):
                continue
            if self._extract_content(message) is not None:
                return True
        return False

    @staticmethod
    def _extract_messages(data: dict[str, Any]) -> list[Any]:
        mapping = data.get("mapping")
        if isinstance(mapping, dict):
            nodes: list[dict[str, Any]] = []
            for key, node in mapping.items():
                if not isinstance(node, dict):
                    continue
                message = node.get("message")
                if not isinstance(message, dict):
                    continue
                item = dict(message)
                item.setdefault("id", node.get("id") or key)
                item.setdefault("parent", node.get("parent"))
                nodes.append(item)
            nodes.sort(
                key=lambda msg: (
                    DeepseekConversationImporter._pick_timestamp(
                        msg,
                        "inserted_at",
                        "created_at",
                        "create_time",
                    ),
                    str(msg.get("id") or ""),
                )
            )
            if nodes:
                return nodes

        messages = data.get("messages")
        if isinstance(messages, list):
            return messages
        conversation = data.get("conversation")
        if isinstance(conversation, dict) and isinstance(conversation.get("messages"), list):
            return conversation["messages"]
        chat = data.get("chat")
        if isinstance(chat, dict) and isinstance(chat.get("messages"), list):
            return chat["messages"]
        return []

    @staticmethod
    def _extract_role(message: dict[str, Any]) -> str:
        role = message.get("role")
        if isinstance(role, str) and role.strip():
            return role.strip()

        author = message.get("author")
        if isinstance(author, dict):
            role = author.get("role")
            if isinstance(role, str) and role.strip():
                return role.strip()
            name = author.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
        if isinstance(author, str) and author.strip():
            return author.strip()

        sender = message.get("sender")
        if isinstance(sender, str) and sender.strip():
            return sender.strip()
        fragment_role = DeepseekConversationImporter._role_from_fragments(message.get("fragments"))
        if fragment_role:
            return fragment_role
        return "unknown"

    @staticmethod
    def _role_from_fragments(fragments: Any) -> str | None:
        if not isinstance(fragments, list):
            return None
        for fragment in fragments:
            if not isinstance(fragment, dict):
                continue
            frag_type = fragment.get("type")
            if not isinstance(frag_type, str):
                continue
            upper = frag_type.strip().upper()
            if upper in {"REQUEST", "USER", "HUMAN"}:
                return "user"
            if upper in {"RESPONSE", "ASSISTANT", "MODEL", "THINK", "SEARCH", "READ_LINK"}:
                return "assistant"
            if upper == "SYSTEM":
                return "system"
        return None

    def _extract_content(self, message: dict[str, Any]) -> str | None:
        candidates = (
            message.get("content"),
            message.get("text"),
            message.get("message"),
            message.get("result"),
            message.get("output"),
            message.get("response"),
            message.get("fragments"),
        )
        for item in candidates:
            content = self._normalize_content(item)
            if content is not None:
                return content
        return None

    def _normalize_content(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            parts: list[str] = []
            for item in value:
                text = self._normalize_content(item)
                if text is not None:
                    parts.append(text)
            return "\n".join(parts) if parts else None
        if isinstance(value, dict):
            for key in ("text", "content", "result", "output", "response", "value"):
                item = value.get(key)
                text = self._normalize_content(item)
                if text is not None:
                    return text
            for key in ("parts", "chunks", "messages", "fragments"):
                item = value.get(key)
                if isinstance(item, list):
                    text = self._normalize_content(item)
                    if text is not None:
                        return text
        return None

    @staticmethod
    def _pick_timestamp(data: dict[str, Any], *keys: str) -> int:
        for key in keys:
            if key not in data:
                continue
            raw = data.get(key)
            value = safe_int(raw, default=-1)
            if value >= 0:
                return value
            if isinstance(raw, str):
                parsed = DeepseekConversationImporter._parse_datetime(raw)
                if parsed > 0:
                    return parsed
        return 0

    @staticmethod
    def _parse_datetime(raw: str) -> int:
        text = raw.strip()
        if not text:
            return 0
        try:
            # Common format: 2026-02-27T10:42:00.123Z
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            return int(datetime.fromisoformat(text).timestamp())
        except ValueError:
            return 0
