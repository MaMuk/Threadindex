from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable
from pathlib import Path

from .models import ConversationData, MessageData


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.conn.close()

    def commit(self) -> None:
        self.conn.commit()

    def initialize(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                title TEXT,
                created_at INTEGER,
                updated_at INTEGER,
                source TEXT,
                content_hash TEXT,
                message_count INTEGER
            );

            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                conversation_id TEXT,
                author_role TEXT,
                created_at INTEGER,
                content TEXT,
                content_hash TEXT,
                sequence INTEGER,
                FOREIGN KEY(conversation_id) REFERENCES conversations(id)
            );

            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS conversation_tags (
                conversation_id TEXT,
                tag_id INTEGER,
                PRIMARY KEY(conversation_id, tag_id),
                FOREIGN KEY(conversation_id) REFERENCES conversations(id),
                FOREIGN KEY(tag_id) REFERENCES tags(id)
            );

            CREATE TABLE IF NOT EXISTS import_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                imported_at INTEGER,
                source TEXT,
                source_hash TEXT,
                inserted_conversations INTEGER,
                updated_conversations INTEGER,
                inserted_messages INTEGER,
                updated_messages INTEGER,
                skipped INTEGER
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS conversation_fts
            USING fts5(title, content, tags, conversation_id UNINDEXED);

            CREATE INDEX IF NOT EXISTS idx_messages_conversation_id
            ON messages(conversation_id);

            CREATE INDEX IF NOT EXISTS idx_messages_sequence
            ON messages(conversation_id, sequence);

            CREATE INDEX IF NOT EXISTS idx_conversations_updated_at
            ON conversations(updated_at);
            """
        )
        self.conn.commit()

    def insert_conversation(self, conversation: ConversationData) -> None:
        self.conn.execute(
            """
            INSERT INTO conversations
            (id, title, created_at, updated_at, source, content_hash, message_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                conversation.id,
                conversation.title,
                conversation.created_at,
                conversation.updated_at,
                conversation.source,
                conversation.content_hash,
                conversation.message_count,
            ),
        )

    def update_conversation(self, conversation: ConversationData) -> None:
        self.conn.execute(
            """
            UPDATE conversations
            SET title = ?, created_at = ?, updated_at = ?, source = ?,
                content_hash = ?, message_count = ?
            WHERE id = ?
            """,
            (
                conversation.title,
                conversation.created_at,
                conversation.updated_at,
                conversation.source,
                conversation.content_hash,
                conversation.message_count,
                conversation.id,
            ),
        )

    def upsert_message(self, message: MessageData) -> None:
        self.conn.execute(
            """
            INSERT INTO messages
            (id, conversation_id, author_role, created_at, content, content_hash, sequence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                conversation_id = excluded.conversation_id,
                author_role = excluded.author_role,
                created_at = excluded.created_at,
                content = excluded.content,
                content_hash = excluded.content_hash,
                sequence = excluded.sequence
            """,
            (
                message.id,
                message.conversation_id,
                message.author_role,
                message.created_at,
                message.content,
                message.content_hash,
                message.sequence,
            ),
        )

    def insert_messages(self, messages: Iterable[MessageData]) -> None:
        for message in messages:
            self.upsert_message(message)

    def get_conversation_meta(self, conversation_id: str) -> sqlite3.Row | None:
        cur = self.conn.execute(
            "SELECT * FROM conversations WHERE id = ?",
            (conversation_id,),
        )
        return cur.fetchone()

    def get_message_hashes(self, conversation_id: str) -> dict[str, str]:
        cur = self.conn.execute(
            "SELECT id, content_hash FROM messages WHERE conversation_id = ?",
            (conversation_id,),
        )
        return {row["id"]: row["content_hash"] for row in cur.fetchall()}

    def list_conversations(
        self,
        search: str | None = None,
        tags: list[str] | None = None,
        updated_from: int | None = None,
        updated_to: int | None = None,
        created_from: int | None = None,
        created_to: int | None = None,
        title: str | None = None,
        source: str | None = None,
        conv_id: str | None = None,
        message_min: int | None = None,
        message_max: int | None = None,
        sort_by: str = "updated",
        sort_order: str = "desc",
        limit: int = 1000,
    ) -> list[dict]:
        sort_map = {
            "updated": "c.updated_at",
            "created": "c.created_at",
            "title": "c.title",
            "messages": "c.message_count",
        }
        sort_col = sort_map.get(sort_by, "c.updated_at")
        order = "DESC" if sort_order.lower() == "desc" else "ASC"

        joins = [
            "LEFT JOIN conversation_tags ct ON ct.conversation_id = c.id",
            "LEFT JOIN tags t ON t.id = ct.tag_id",
        ]
        where_clauses = []
        params: list[object] = []

        query = self._fts_query(search) if search else ""
        if query:
            joins.insert(0, "JOIN conversation_fts f ON c.id = f.conversation_id")
            where_clauses.append("conversation_fts MATCH ?")
            params.append(query)

        if tags:
            placeholders = ", ".join("?" for _ in tags)
            where_clauses.append(
                f"""c.id IN (
                    SELECT ct2.conversation_id
                    FROM conversation_tags ct2
                    JOIN tags t2 ON t2.id = ct2.tag_id
                    WHERE t2.name IN ({placeholders})
                )"""
            )
            params.extend(tags)

        if updated_from is not None:
            where_clauses.append("c.updated_at >= ?")
            params.append(updated_from)
        if updated_to is not None:
            where_clauses.append("c.updated_at <= ?")
            params.append(updated_to)
        if created_from is not None:
            where_clauses.append("c.created_at >= ?")
            params.append(created_from)
        if created_to is not None:
            where_clauses.append("c.created_at <= ?")
            params.append(created_to)
        if title:
            where_clauses.append("LOWER(c.title) LIKE ?")
            params.append(f"%{title.lower()}%")
        if source:
            where_clauses.append("LOWER(c.source) LIKE ?")
            params.append(f"%{source.lower()}%")
        if conv_id:
            where_clauses.append("LOWER(c.id) LIKE ?")
            params.append(f"%{conv_id.lower()}%")
        if message_min is not None:
            where_clauses.append("c.message_count >= ?")
            params.append(message_min)
        if message_max is not None:
            where_clauses.append("c.message_count <= ?")
            params.append(message_max)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        join_sql = "\n".join(joins)

        sql = f"""
            SELECT c.*, GROUP_CONCAT(t.name, ',') AS tags
            FROM conversations c
            {join_sql}
            {where_sql}
            GROUP BY c.id
            ORDER BY {sort_col} {order}
            LIMIT ?
        """
        params.append(limit)
        cur = self.conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def get_conversation(self, conversation_id: str) -> dict | None:
        cur = self.conn.execute(
            """
            SELECT c.*, GROUP_CONCAT(t.name, ',') AS tags
            FROM conversations c
            LEFT JOIN conversation_tags ct ON ct.conversation_id = c.id
            LEFT JOIN tags t ON t.id = ct.tag_id
            WHERE c.id = ?
            GROUP BY c.id
            """,
            (conversation_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return dict(row)

    def get_messages(self, conversation_id: str) -> list[dict]:
        cur = self.conn.execute(
            """
            SELECT * FROM messages
            WHERE conversation_id = ?
            ORDER BY sequence ASC, created_at ASC
            """,
            (conversation_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    def set_tags(self, conversation_id: str, tag_names: list[str]) -> None:
        cleaned = [name.strip() for name in tag_names if name.strip()]
        self.conn.execute(
            "DELETE FROM conversation_tags WHERE conversation_id = ?",
            (conversation_id,),
        )
        for name in cleaned:
            self.conn.execute("INSERT OR IGNORE INTO tags(name) VALUES (?)", (name,))
            tag_id = self.conn.execute(
                "SELECT id FROM tags WHERE name = ?",
                (name,),
            ).fetchone()["id"]
            self.conn.execute(
                "INSERT OR IGNORE INTO conversation_tags(conversation_id, tag_id) VALUES (?, ?)",
                (conversation_id, tag_id),
            )

    def get_tags(self, conversation_id: str) -> list[str]:
        cur = self.conn.execute(
            """
            SELECT t.name
            FROM tags t
            JOIN conversation_tags ct ON ct.tag_id = t.id
            WHERE ct.conversation_id = ?
            ORDER BY t.name ASC
            """,
            (conversation_id,),
        )
        return [row["name"] for row in cur.fetchall()]

    def update_fts(self, conversation_id: str) -> None:
        conversation = self.get_conversation(conversation_id)
        if not conversation:
            return
        messages = self.get_messages(conversation_id)
        content = "\n".join(m["content"] or "" for m in messages)
        tags = ",".join(self.get_tags(conversation_id))
        self.conn.execute(
            "DELETE FROM conversation_fts WHERE conversation_id = ?",
            (conversation_id,),
        )
        self.conn.execute(
            """
            INSERT INTO conversation_fts(title, content, tags, conversation_id)
            VALUES (?, ?, ?, ?)
            """,
            (conversation["title"], content, tags, conversation_id),
        )

    def rebuild_fts(self) -> int:
        self.conn.execute("DELETE FROM conversation_fts")
        cur = self.conn.execute("SELECT id FROM conversations")
        count = 0
        for row in cur.fetchall():
            self.update_fts(row["id"])
            count += 1
        return count

    def add_import_run(
        self,
        imported_at: int,
        source: str,
        source_hash: str,
        inserted_conversations: int,
        updated_conversations: int,
        inserted_messages: int,
        updated_messages: int,
        skipped: int,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO import_runs
            (imported_at, source, source_hash, inserted_conversations, updated_conversations,
             inserted_messages, updated_messages, skipped)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                imported_at,
                source,
                source_hash,
                inserted_conversations,
                updated_conversations,
                inserted_messages,
                updated_messages,
                skipped,
            ),
        )

    def doctor_info(self) -> dict:
        cur = self.conn.execute("SELECT COUNT(*) AS count FROM conversations")
        conversation_count = cur.fetchone()["count"]
        cur = self.conn.execute("SELECT COUNT(*) AS count FROM messages")
        message_count = cur.fetchone()["count"]
        cur = self.conn.execute("SELECT COUNT(*) AS count FROM conversation_fts")
        fts_count = cur.fetchone()["count"]
        return {
            "conversations": conversation_count,
            "messages": message_count,
            "fts_rows": fts_count,
        }

    @staticmethod
    def _fts_query(search: str) -> str:
        if not search:
            return ""
        tokens = [token for token in re.findall(r"[\\w\\-]+", search) if token]
        if not tokens:
            return ""
        if len(tokens) == 1:
            return tokens[0]
        return " AND ".join(tokens)
