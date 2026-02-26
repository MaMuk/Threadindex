from __future__ import annotations

import json
import re
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import Database
from .logging import get_logger
from .models import ConversationData, ImportResult, MessageData
from .utils import (
    canonical_json,
    fingerprint_conversation,
    fingerprint_message,
    safe_int,
    sha256_bytes,
)

_LOG = get_logger()


@dataclass(frozen=True)
class ImportStats:
    inserted_conversations: int = 0
    updated_conversations: int = 0
    inserted_messages: int = 0
    updated_messages: int = 0
    skipped: int = 0


class Importer:
    def __init__(self, db: Database):
        self.db = db

    def import_path(self, path: Path, dry_run: bool = False) -> ImportResult:
        path = path.expanduser()
        source_hash = self._compute_source_hash(path)
        raw_conversations = self._load_conversations(path)
        conversations = [self._normalize_conversation(raw, str(path)) for raw in raw_conversations]

        stats = ImportStats()

        if not dry_run:
            with self.db.conn:
                for conversation in conversations:
                    stats = self._upsert_conversation(conversation, stats)
                self.db.add_import_run(
                    imported_at=int(time.time()),
                    source=str(path),
                    source_hash=source_hash,
                    inserted_conversations=stats.inserted_conversations,
                    updated_conversations=stats.updated_conversations,
                    inserted_messages=stats.inserted_messages,
                    updated_messages=stats.updated_messages,
                    skipped=stats.skipped,
                )
                self.db.commit()
        else:
            for conversation in conversations:
                stats = self._upsert_conversation(conversation, stats, dry_run=True)

        return ImportResult(
            inserted_conversations=stats.inserted_conversations,
            updated_conversations=stats.updated_conversations,
            inserted_messages=stats.inserted_messages,
            updated_messages=stats.updated_messages,
            skipped=stats.skipped,
            source_hash=source_hash,
        )

    def _upsert_conversation(
        self,
        conversation: ConversationData,
        stats: ImportStats,
        dry_run: bool = False,
    ) -> ImportStats:
        existing = self.db.get_conversation_meta(conversation.id)
        if existing is None:
            return self._insert_conversation(conversation, stats, dry_run)

        if existing["content_hash"] == conversation.content_hash:
            return ImportStats(
                inserted_conversations=stats.inserted_conversations,
                updated_conversations=stats.updated_conversations,
                inserted_messages=stats.inserted_messages,
                updated_messages=stats.updated_messages,
                skipped=stats.skipped + 1,
            )

        updated_conversations = stats.updated_conversations + 1
        message_hashes = self.db.get_message_hashes(conversation.id)

        inserted_messages = stats.inserted_messages
        updated_messages = stats.updated_messages

        for message in conversation.messages:
            existing_hash = message_hashes.get(message.id)
            if existing_hash is None:
                inserted_messages += 1
            elif existing_hash != message.content_hash:
                updated_messages += 1

            if not dry_run:
                self.db.upsert_message(message)

        created_at = conversation.created_at or existing["created_at"]
        updated_at = max(conversation.updated_at, existing["updated_at"] or 0)

        updated_conversation = ConversationData(
            id=conversation.id,
            title=conversation.title,
            created_at=created_at,
            updated_at=updated_at,
            source=conversation.source,
            content_hash=conversation.content_hash,
            message_count=conversation.message_count,
            messages=conversation.messages,
        )

        if not dry_run:
            self.db.update_conversation(updated_conversation)
            self.db.update_fts(conversation.id)

        return ImportStats(
            inserted_conversations=stats.inserted_conversations,
            updated_conversations=updated_conversations,
            inserted_messages=inserted_messages,
            updated_messages=updated_messages,
            skipped=stats.skipped,
        )

    def _insert_conversation(
        self,
        conversation: ConversationData,
        stats: ImportStats,
        dry_run: bool = False,
    ) -> ImportStats:
        if not dry_run:
            self.db.insert_conversation(conversation)
            self.db.insert_messages(conversation.messages)
            self.db.update_fts(conversation.id)

        return ImportStats(
            inserted_conversations=stats.inserted_conversations + 1,
            updated_conversations=stats.updated_conversations,
            inserted_messages=stats.inserted_messages + conversation.message_count,
            updated_messages=stats.updated_messages,
            skipped=stats.skipped,
        )

    def _compute_source_hash(self, path: Path) -> str:
        if path.is_file():
            return sha256_bytes(path.read_bytes())
        if path.is_dir():
            conv_files = self._find_conversation_files(path)
            if conv_files:
                payload = {"files": [str(file) for file in conv_files]}
                return sha256_bytes(canonical_json(payload).encode("utf-8"))
            payload = {
                "files": sorted(
                    (
                        (str(p.relative_to(path)), p.stat().st_size, int(p.stat().st_mtime))
                        for p in path.rglob("*")
                        if p.is_file()
                    ),
                    key=lambda x: x[0],
                )
            }
            return sha256_bytes(canonical_json(payload).encode("utf-8"))
        raise FileNotFoundError(f"Path not found: {path}")

    def _load_conversations(self, path: Path) -> list[dict[str, Any]]:
        if path.is_file() and path.suffix.lower() == ".zip":
            return self._load_from_zip(path)
        if path.is_dir():
            return self._load_from_directory(path)
        if path.is_file() and path.suffix.lower() == ".json":
            return self._load_from_json_file(path)
        raise ValueError(f"Unsupported import path: {path}")

    def _load_from_zip(self, path: Path) -> list[dict[str, Any]]:
        conversations: list[dict[str, Any]] = []
        with zipfile.ZipFile(path) as zf:
            names = [name for name in zf.namelist() if name.lower().endswith(".json")]
            conv_names = [name for name in names if self._is_conversation_file(name)]
            targets = sorted(conv_names or names)
            for name in targets:
                try:
                    text = zf.read(name).decode("utf-8-sig")
                except UnicodeDecodeError as exc:
                    _LOG.warning("Skipping %s (%s)", name, exc)
                    continue
                conversations.extend(self._load_from_json_text(text))
        if not conversations:
            raise ValueError("No conversations found in zip export")
        return conversations

    def _load_from_directory(self, path: Path) -> list[dict[str, Any]]:
        conv_files = self._find_conversation_files(path)
        if conv_files:
            conversations: list[dict[str, Any]] = []
            for file in conv_files:
                conversations.extend(self._load_from_json_file(file))
            return conversations

        conversations: list[dict[str, Any]] = []
        for file in path.rglob("*.json"):
            try:
                conversations.extend(self._load_from_json_file(file))
            except (ValueError, json.JSONDecodeError):
                continue
        if not conversations:
            raise ValueError("No conversations found in directory")
        return conversations

    def _find_conversation_files(self, path: Path) -> list[Path]:
        files = [file for file in path.rglob("*.json") if self._is_conversation_file(file.name)]
        return sorted(files)

    @staticmethod
    def _is_conversation_file(name: str) -> bool:
        lower = name.lower()
        base = lower.rsplit("/", 1)[-1]
        if base == "conversations.json":
            return True
        return re.fullmatch(r"conversations-\d+\.json", base) is not None

    def _load_from_json_file(self, path: Path) -> list[dict[str, Any]]:
        text = path.read_text(encoding="utf-8-sig")
        return self._load_from_json_text(text)

    def _load_from_json_text(self, text: str) -> list[dict[str, Any]]:
        try:
            data = json.loads(text)
            return self._normalize_json_payload(data)
        except json.JSONDecodeError:
            conversations: list[dict[str, Any]] = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                conversations.extend(self._normalize_json_payload(item))
            return conversations

    def _normalize_json_payload(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            if "conversations" in data and isinstance(data["conversations"], list):
                return [item for item in data["conversations"] if isinstance(item, dict)]
            if "conversations" in data and isinstance(data["conversations"], dict):
                nested = data["conversations"]
                if isinstance(nested.get("items"), list):
                    return [item for item in nested["items"] if isinstance(item, dict)]
                if isinstance(nested.get("data"), list):
                    return [item for item in nested["data"] if isinstance(item, dict)]
            if "items" in data and isinstance(data["items"], list):
                return [item for item in data["items"] if isinstance(item, dict)]
            if "data" in data and isinstance(data["data"], list):
                return [item for item in data["data"] if isinstance(item, dict)]
            if "mapping" in data:
                return [data]
        return []

    def _normalize_conversation(self, raw: dict[str, Any], source: str) -> ConversationData:
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
            source=source,
            content_hash=content_hash,
            message_count=len(message_data),
            messages=message_data,
        )

    def _extract_content(self, message: dict[str, Any]) -> str | None:
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
