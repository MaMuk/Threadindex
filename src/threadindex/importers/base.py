from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ..models import ConversationData


@dataclass(frozen=True)
class RawConversationRecord:
    source_type: str
    payload: dict[str, Any]


class ConversationSourceImporter(Protocol):
    source_type: str

    def extract_conversations(self, payload: Any) -> list[dict[str, Any]]:
        """Return source-native conversation objects if the payload matches."""

    def normalize_conversation(self, raw: dict[str, Any], file_source: str) -> ConversationData:
        """Convert source-native conversation JSON into internal model."""
