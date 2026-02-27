from __future__ import annotations

import json
import re
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .db import Database
from .importers import (
    ChatGPTConversationImporter,
    ConversationSourceImporter,
    DeepseekConversationImporter,
    RawConversationRecord,
)
from .logging import get_logger
from .models import ConversationData, ImportResult
from .utils import canonical_json, sha256_bytes

_LOG = get_logger()
SOURCE_CHOICES = {"auto", "chatgpt", "deepseek"}


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
        self.source_importers: list[ConversationSourceImporter] = [
            ChatGPTConversationImporter(),
            DeepseekConversationImporter(),
        ]

    def import_path(self, path: Path, dry_run: bool = False, source: str = "auto") -> ImportResult:
        path = path.expanduser()
        selected_source = source.lower().strip()
        if selected_source not in SOURCE_CHOICES:
            raise ValueError(f"Unsupported source: {source}. Use one of: auto, chatgpt, deepseek")
        source_hash = self._compute_source_hash(path)
        raw_records = self._load_conversations(path, source=selected_source)
        conversations = self._normalize_records(raw_records, str(path))

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
            file_source=conversation.file_source,
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

    def _load_conversations(self, path: Path, source: str = "auto") -> list[RawConversationRecord]:
        if path.is_file() and path.suffix.lower() == ".zip":
            return self._load_from_zip(path, source=source)
        if path.is_dir():
            return self._load_from_directory(path, source=source)
        if path.is_file() and path.suffix.lower() == ".json":
            return self._load_from_json_file(path, source=source)
        raise ValueError(f"Unsupported import path: {path}")

    def _load_from_zip(self, path: Path, source: str = "auto") -> list[RawConversationRecord]:
        records: list[RawConversationRecord] = []
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
                records.extend(self._load_from_json_text(text, source=source))
        if not records:
            raise ValueError("No conversations found in zip export")
        return records

    def _load_from_directory(self, path: Path, source: str = "auto") -> list[RawConversationRecord]:
        conv_files = self._find_conversation_files(path)
        if conv_files:
            records: list[RawConversationRecord] = []
            for file in conv_files:
                records.extend(self._load_from_json_file(file, source=source))
            return records

        records: list[RawConversationRecord] = []
        for file in path.rglob("*.json"):
            try:
                records.extend(self._load_from_json_file(file, source=source))
            except (ValueError, json.JSONDecodeError):
                continue
        if not records:
            raise ValueError("No conversations found in directory")
        return records

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

    def _load_from_json_file(self, path: Path, source: str = "auto") -> list[RawConversationRecord]:
        text = path.read_text(encoding="utf-8-sig")
        return self._load_from_json_text(text, source=source)

    def _load_from_json_text(self, text: str, source: str = "auto") -> list[RawConversationRecord]:
        try:
            data = json.loads(text)
            return self._extract_records_from_payload(data, source=source)
        except json.JSONDecodeError:
            records: list[RawConversationRecord] = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                records.extend(self._extract_records_from_payload(item, source=source))
            return records

    def _extract_records_from_payload(
        self,
        payload: Any,
        source: str = "auto",
    ) -> list[RawConversationRecord]:
        records: list[RawConversationRecord] = []
        candidates = self.source_importers
        if source != "auto":
            candidates = [item for item in self.source_importers if item.source_type == source]

        for source_importer in candidates:
            conversations = source_importer.extract_conversations(payload)
            if not conversations:
                continue
            records.extend(
                RawConversationRecord(source_type=source_importer.source_type, payload=item)
                for item in conversations
            )
            return records
        return records

    def _normalize_records(
        self,
        records: list[RawConversationRecord],
        source_path: str,
    ) -> list[ConversationData]:
        if not records:
            return []

        importer_map = {source.source_type: source for source in self.source_importers}
        conversations: list[ConversationData] = []

        for record in records:
            source_importer = importer_map.get(record.source_type)
            if source_importer is None:
                _LOG.warning("Skipping unsupported source type: %s", record.source_type)
                continue
            normalized = source_importer.normalize_conversation(record.payload, source_path)
            if normalized.message_count == 0:
                _LOG.debug(
                    "Skipping %s conversation without messages (path=%s)",
                    record.source_type,
                    source_path,
                )
                continue
            conversations.append(normalized)

        if not conversations:
            raise ValueError("No supported conversations found in input")
        return conversations
