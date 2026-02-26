from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MessageData:
    id: str
    conversation_id: str
    author_role: str
    content: str
    created_at: int
    content_hash: str
    sequence: int


@dataclass(frozen=True)
class ConversationData:
    id: str
    title: str
    created_at: int
    updated_at: int
    source: str
    content_hash: str
    message_count: int
    messages: list[MessageData]


@dataclass(frozen=True)
class ImportResult:
    inserted_conversations: int
    updated_conversations: int
    inserted_messages: int
    updated_messages: int
    skipped: int
    source_hash: str
