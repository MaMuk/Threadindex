from __future__ import annotations

import shlex
from datetime import datetime
from pathlib import Path

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Footer, Header, Input, Static

try:
    from textual.widgets import TextLog
except ImportError:  # pragma: no cover - fallback for newer Textual
    from textual.widgets import RichLog as TextLog

from .config import load_config, load_db_history, save_config, save_db_history
from .db import Database
from .importer import Importer
from .utils import fingerprint_conversation


class HelpScreen(ModalScreen[None]):
    BINDINGS = [("escape", "close", "Close"), ("q", "close", "Close")]

    def compose(self) -> ComposeResult:
        help_text = (
            "Keyboard\n"
            "- arrows: Move selection\n"
            "- Enter: Focus preview\n"
            "- Esc: Focus list\n"
            "- /: Search\n"
            "- t: Tag conversation\n"
            "- d: Details\n"
            "- s: Cycle sort field\n"
            "- o: Toggle sort order\n"
            "- f: Filter dialog\n"
            "- g: Settings\n"
            "- : Command bar\n"
            "- q: Quit\n"
            "\n"
            "Command bar examples\n"
            "- retitle New title\n"
            "- sort updated desc\n"
            "- filter tag work,ideas\n"
            "- filter updated 2024-01-01 2024-12-31\n"
            "- filter created 2024-01-01 2024-12-31\n"
            "- filter title \"planning\"\n"
            "- filter clear\n"
            "- details\n"
        )
        yield Static(help_text, id="help_text")

    def action_close(self) -> None:
        self.app.pop_screen()


class DetailsScreen(ModalScreen[None]):
    BINDINGS = [("escape", "close", "Close"), ("q", "close", "Close")]

    def __init__(self, details: str) -> None:
        super().__init__()
        self._details = details

    def compose(self) -> ComposeResult:
        yield Static(self._details, id="details_text")

    def action_close(self) -> None:
        self.app.pop_screen()


FILTER_FIELDS = [
    ("title", "Title", "Text contains"),
    ("tags", "Tags", "Comma-separated tags"),
    ("updated", "Updated", "Date range: YYYY-MM-DD YYYY-MM-DD"),
    ("created", "Created", "Date range: YYYY-MM-DD YYYY-MM-DD"),
    ("messages", "Messages", "Range: 5 20"),
    ("source", "Source", "Text contains"),
    ("id", "ID", "Text contains"),
]


class FilterScreen(ModalScreen[None]):
    BINDINGS = [
        ("escape", "close", "Close"),
        ("q", "close", "Close"),
        ("c", "clear", "Clear"),
        ("enter", "apply_or_edit", "Apply"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.current_field = FILTER_FIELDS[0][0]
        self.status: str | None = None

    def compose(self) -> ComposeResult:
        with Vertical(id="filter_panel"):
            yield Static("Filters", id="filter_title")
            yield DataTable(id="filter_table")
            yield Static("", id="filter_hint")
            yield Input(placeholder="Value (Enter to apply, empty to clear)", id="filter_value")

    def on_mount(self) -> None:
        self.table = self.query_one("#filter_table", DataTable)
        self.hint = self.query_one("#filter_hint", Static)
        self.value = self.query_one("#filter_value", Input)

        self.table.add_columns("Field", "Current")
        self.table.cursor_type = "row"
        self.table.zebra_stripes = True
        self._refresh_table()
        self.value.value = self.app._filter_display(self.current_field)
        if self.table.row_count:
            self.table.cursor_coordinate = (0, 0)
        self.table.focus()
        self._update_hint()

    def action_close(self) -> None:
        self.app.pop_screen()

    def action_clear(self) -> None:
        self.app._clear_filters()
        self.status = "Cleared all filters."
        self._refresh_table()
        self._update_hint()

    def action_apply_or_edit(self) -> None:
        if self.value.has_focus:
            self._apply_current()
        else:
            self.value.focus()

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key:
            self.current_field = event.row_key.value
            self.value.value = self.app._filter_display(self.current_field)
            self._update_hint()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.value.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "filter_value":
            self._apply_current()

    def _apply_current(self) -> None:
        raw = self.value.value
        ok, message = self.app._apply_filter_field(self.current_field, raw)
        self.status = message
        if ok:
            self._refresh_table()
        self._update_hint()

    def _update_hint(self) -> None:
        hint = next((item[2] for item in FILTER_FIELDS if item[0] == self.current_field), "")
        status = f" | {self.status}" if self.status else ""
        self.hint.update(f"{self.current_field}: {hint}{status}")

    def _refresh_table(self) -> None:
        self.table.clear()
        for key, label, _ in FILTER_FIELDS:
            current = self.app._filter_display(key)
            self.table.add_row(label, current, key=key)


class SettingsScreen(ModalScreen[None]):
    BINDINGS = [
        ("escape", "close", "Close"),
        ("q", "close", "Close"),
        ("a", "add_db", "Add DB"),
        ("enter", "select_db", "Select"),
        ("i", "import_data", "Import"),
        ("r", "reindex", "Reindex"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.input_mode: str | None = None
        self.status: str | None = None
        self.row_paths: list[str] = []

    def compose(self) -> ComposeResult:
        with Vertical(id="settings_panel"):
            yield Static("Settings", id="settings_title")
            yield Static("", id="settings_current")
            yield Static(
                "Keys: a add db | enter select | i import | r reindex | q/esc close",
                id="settings_keys",
            )
            yield DataTable(id="settings_table")
            yield Static("", id="settings_hint")
            with Horizontal(id="settings_input_row"):
                yield Input(placeholder="Enter to apply", id="settings_input")
                yield Button("Apply", id="settings_apply")

    def on_mount(self) -> None:
        self.table = self.query_one("#settings_table", DataTable)
        self.hint = self.query_one("#settings_hint", Static)
        self.input = self.query_one("#settings_input", Input)
        self.current = self.query_one("#settings_current", Static)
        self.apply_button = self.query_one("#settings_apply", Button)

        self.table.add_columns("Database", "Status")
        self.table.cursor_type = "row"
        self.table.zebra_stripes = True
        self._refresh_table()
        if self.table.row_count:
            self.table.cursor_coordinate = (0, 0)
        self.table.focus()
        self._update_hint()

    def action_close(self) -> None:
        self.app.pop_screen()

    def action_add_db(self) -> None:
        self.input_mode = "db"
        self.input.placeholder = "Database path (Enter to select)"
        self.input.value = ""
        self.input.display = True
        self.input.focus()
        self._update_hint("Enter a path for a new or existing database.")

    def action_select_db(self) -> None:
        if self.table.row_count == 0:
            return
        idx = self.table.cursor_row
        if idx is None or idx < 0 or idx >= len(self.row_paths):
            return
        path = Path(self.row_paths[idx])
        self.app._switch_db(path)
        self._refresh_table()
        self._update_hint("Database selected.")

    def action_import_data(self) -> None:
        self.input_mode = "import"
        self.input.placeholder = "Import path (zip/dir/json)"
        self.input.value = ""
        self.input.display = True
        self.input.focus()
        self._update_hint("Import into the current database.")

    def action_reindex(self) -> None:
        count = self.app.db.rebuild_fts()
        self.app.db.commit()
        self._update_hint(f"Reindexed {count} conversations.")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "settings_input":
            return
        self._submit_input(event.value)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "settings_apply":
            self._submit_input(self.input.value)

    def on_key(self, event) -> None:  # type: ignore[override]
        if event.key == "a":
            event.stop()
            self.action_add_db()
        elif event.key == "i":
            event.stop()
            self.action_import_data()
        elif event.key == "r":
            event.stop()
            self.action_reindex()
        elif event.key == "enter" and self.table.has_focus:
            event.stop()
            self.action_select_db()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.action_select_db()

    def _refresh_table(self) -> None:
        self.table.clear()
        self.row_paths = []
        current = self.app.config.paths.db_path
        self.current.update(f"Current DB: {current}")
        self.app.db_history = self.app._dedupe_paths([current] + self.app.db_history)
        self.app._save_db_history()
        for path in self.app.db_history:
            status = "current" if path == current else ""
            self.table.add_row(str(path), status, key=str(path))
            self.row_paths.append(str(path))

    def _update_hint(self, message: str | None = None) -> None:
        if message:
            self.status = message
        hint = self.status or "Select a database or press a/i/r for actions."
        self.hint.update(hint)

    def _submit_input(self, raw: str) -> None:
        value = raw.strip()
        if self.input_mode == "db":
            if value:
                self.app._switch_db(Path(value))
                self._refresh_table()
                self._update_hint("Database added and selected.")
            else:
                self._update_hint("No path entered.")
        elif self.input_mode == "import":
            if value:
                self.app._perform_import(value)
                self._refresh_table()
                self._update_hint("Import complete.")
            else:
                self._update_hint("No import path entered.")
        else:
            self._update_hint("No action selected.")
        self.input_mode = None
        self.table.focus()

class ThreadIndexApp(App):
    CSS = """
    Screen {
        layout: vertical;
    }

    #main {
        height: 1fr;
    }

    #list {
        width: 42%;
        min-width: 30;
        border-right: heavy $surface;
    }

    #preview {
        width: 1fr;
        padding: 1 2;
    }

    #command_input {
        dock: bottom;
        height: 3;
        padding: 0 1;
        display: none;
    }

    #help_text, #details_text {
        padding: 2 4;
        width: 80%;
        background: $panel;
        border: heavy $primary;
    }

    #filter_panel {
        padding: 2 4;
        width: 80%;
        height: 70%;
        background: $panel;
        border: heavy $primary;
    }

    #filter_table {
        height: 1fr;
    }

    #filter_hint {
        height: 3;
        padding: 1 0;
    }

    #settings_panel {
        padding: 2 4;
        width: 80%;
        height: 70%;
        background: $panel;
        border: heavy $primary;
    }

    #settings_current {
        height: 2;
        padding: 0 0 1 0;
    }

    #settings_keys {
        height: 2;
        padding: 0 0 1 0;
    }

    #settings_table {
        height: 1fr;
    }

    #settings_hint {
        height: 3;
        padding: 1 0;
    }

    #settings_input_row {
        height: 3;
    }

    #settings_input {
        width: 1fr;
    }

    #settings_apply {
        width: 12;
        margin-left: 1;
    }
    """

    BINDINGS = [
        ("down", "move_down", "Down"),
        ("up", "move_up", "Up"),
        ("/", "search", "Search"),
        ("t", "tag", "Tag"),
        ("d", "details", "Details"),
        ("s", "sort_cycle", "Sort"),
        ("o", "order_toggle", "Order"),
        ("f", "filter", "Filter"),
        ("g", "settings", "Settings"),
        ("?", "help", "Help"),
        (":", "command", "Command"),
        ("enter", "focus_preview", "Preview"),
        ("escape", "focus_list", "List"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.config = load_config()
        self.db = Database(self.config.paths.db_path)
        self.db.initialize()
        self.importer = Importer(self.db)
        self.search_query: str | None = None
        self.input_mode: str | None = None
        self.search_timer = None
        self.row_ids: list[str] = []
        self.filter_tags: list[str] = []
        self.filter_title: str | None = None
        self.filter_source: str | None = None
        self.filter_id: str | None = None
        self.filter_updated_from: int | None = None
        self.filter_updated_to: int | None = None
        self.filter_created_from: int | None = None
        self.filter_created_to: int | None = None
        self.filter_message_min: int | None = None
        self.filter_message_max: int | None = None
        self.sort_by = "created"
        self.sort_order = "desc"
        self.db_history: list[Path] = self._load_db_history()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with Vertical(id="main"):
            with Horizontal():
                yield DataTable(id="list")
                yield TextLog(id="preview", highlight=False, wrap=True)
            yield Input(placeholder="Command", id="command_input")
        yield Footer()

    def on_mount(self) -> None:
        self.table = self.query_one("#list", DataTable)
        self.preview = self.query_one("#preview", TextLog)
        self.command_input = self.query_one("#command_input", Input)

        self.table.add_columns("Created", "Title", "Tags", "#")
        self.table.zebra_stripes = True
        self.table.cursor_type = "row"

        self.load_conversations()
        if self.table.row_count:
            self.table.focus()

    def load_conversations(self, search: str | None = None) -> None:
        self.table.clear()
        self.row_ids = []
        conversations = self.db.list_conversations(
            search=search,
            tags=self.filter_tags,
            updated_from=self.filter_updated_from,
            updated_to=self.filter_updated_to,
            created_from=self.filter_created_from,
            created_to=self.filter_created_to,
            title=self.filter_title,
            source=self.filter_source,
            conv_id=self.filter_id,
            message_min=self.filter_message_min,
            message_max=self.filter_message_max,
            sort_by=self.sort_by,
            sort_order=self.sort_order,
        )
        for conv in conversations:
            date = self._format_ts(conv.get("created_at"))
            title = conv.get("title") or "(untitled)"
            tags = conv.get("tags") or ""
            count = conv.get("message_count") or 0
            self.table.add_row(date, title, tags, str(count), key=conv["id"])
            self.row_ids.append(conv["id"])
        self._update_title()

        if conversations:
            self.table.cursor_coordinate = (0, 0)
            first_id = conversations[0]["id"]
            self.show_preview(first_id)
        else:
            self.preview.clear()
            self.preview.write("No conversations found. Import an export to get started.")

    def show_preview(self, conversation_id: str) -> None:
        conversation = self.db.get_conversation(conversation_id)
        if not conversation:
            return
        messages = self.db.get_messages(conversation_id)

        self.preview.clear()
        self.preview.write(conversation.get("title") or "(untitled)")
        self.preview.write(f"ID: {conversation_id}")
        self.preview.write(f"Created: {self._format_ts(conversation.get('created_at'))}")
        self.preview.write(f"Updated: {self._format_ts(conversation.get('updated_at'))}")
        self.preview.write(f"Tags: {conversation.get('tags') or ''}")
        self.preview.write("")
        for message in messages:
            role = message.get("author_role") or "unknown"
            content = message.get("content") or ""
            self.preview.write(f"[{role}] {content}")
            self.preview.write("")

    def _format_ts(self, ts: int | None) -> str:
        if not ts:
            return ""
        return datetime.fromtimestamp(ts).strftime(self.config.date_format)

    def _chat_url(self, conversation_id: str) -> str:
        base = self.config.chat_url_base or "https://chatgpt.com/c/"
        if not base.endswith("/"):
            base = f"{base}/"
        return f"{base}{conversation_id}"

    def action_move_down(self) -> None:
        if self.preview.has_focus:
            self.preview.scroll_down()
        else:
            self._move_table(1)

    def action_move_up(self) -> None:
        if self.preview.has_focus:
            self.preview.scroll_up()
        else:
            self._move_table(-1)

    def action_focus_preview(self) -> None:
        self.preview.focus()

    def action_focus_list(self) -> None:
        self.table.focus()

    def action_search(self) -> None:
        self.open_input("search", "Search conversations")

    def action_command(self) -> None:
        self.open_input("command", "Command (retitle, sort, filter, details)")

    def action_tag(self) -> None:
        conversation_id = self._current_conversation_id()
        if not conversation_id:
            return
        tags = self.db.get_tags(conversation_id)
        self.open_input("tag", "Comma-separated tags", ", ".join(tags))

    def action_details(self) -> None:
        conversation_id = self._current_conversation_id()
        if not conversation_id:
            return
        conversation = self.db.get_conversation(conversation_id)
        if not conversation:
            return
        details = (
            f"Title: {conversation.get('title') or '(untitled)'}\n"
            f"ID: {conversation_id}\n"
            f"URL: {self._chat_url(conversation_id)}\n"
            f"Created: {self._format_ts(conversation.get('created_at'))}\n"
            f"Updated: {self._format_ts(conversation.get('updated_at'))}\n"
            f"Source: {conversation.get('source') or ''}\n"
            f"Tags: {conversation.get('tags') or ''}\n"
            f"Messages: {conversation.get('message_count') or 0}"
        )
        self.push_screen(DetailsScreen(details))

    def action_help(self) -> None:
        self.push_screen(HelpScreen())

    def action_filter(self) -> None:
        self.push_screen(FilterScreen())

    def action_settings(self) -> None:
        self.push_screen(SettingsScreen())

    def action_sort_cycle(self) -> None:
        options = ["updated", "created", "title", "messages"]
        idx = options.index(self.sort_by) if self.sort_by in options else 0
        self.sort_by = options[(idx + 1) % len(options)]
        self.load_conversations(self.search_query)
        self._notify(f"Sort: {self.sort_by} {self.sort_order}")

    def action_order_toggle(self) -> None:
        self.sort_order = "asc" if self.sort_order == "desc" else "desc"
        self.load_conversations(self.search_query)
        self._notify(f"Sort: {self.sort_by} {self.sort_order}")

    def open_input(self, mode: str, placeholder: str, value: str = "") -> None:
        self.input_mode = mode
        self.command_input.placeholder = placeholder
        self.command_input.value = value
        self.command_input.display = True
        self.command_input.focus()

    def close_input(self) -> None:
        self.command_input.display = False
        self.input_mode = None
        self.table.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if self.input_mode != "search":
            return
        if self.search_timer:
            self.search_timer.stop()
        self.search_timer = self.set_timer(0.2, self._apply_search)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        value = event.value.strip()
        if self.input_mode == "search":
            self.search_query = value or None
            self.load_conversations(self.search_query)
            self.close_input()
            return
        if self.input_mode == "import":
            if value:
                self._perform_import(value)
            self.close_input()
            return
        if self.input_mode == "tag":
            self._apply_tags(value)
            self.close_input()
            return
        if self.input_mode == "command":
            self._handle_command(value)
            self.close_input()
            return

    def _apply_search(self) -> None:
        if self.input_mode != "search":
            return
        value = self.command_input.value.strip()
        self.search_query = value or None
        self.load_conversations(self.search_query)

    def _handle_command(self, value: str) -> None:
        if not value:
            return
        if value.startswith("/"):
            self.search_query = value[1:].strip() or None
            self.load_conversations(self.search_query)
            return
        parts = shlex.split(value)
        if not parts:
            return
        command = parts[0].lower()
        if command == "retitle":
            title = value[len("retitle") :].strip()
            if title:
                self._retitle_current(title)
        elif command == "sort":
            self._apply_sort_command(parts)
        elif command == "filter":
            self._apply_filter_command(value, parts)
        elif command == "clear":
            self._clear_filters()
        elif command == "details":
            self.action_details()
        else:
            self._notify("Unknown command.")

    def _perform_import(self, path_text: str) -> None:
        path = Path(path_text).expanduser()
        try:
            result = self.importer.import_path(path)
        except Exception as exc:  # noqa: BLE001
            self._notify(f"Import failed: {exc}")
            return
        self.load_conversations(self.search_query)
        self._notify(
            f"Imported: {result.inserted_conversations} new, "
            f"{result.updated_conversations} updated, "
            f"{result.inserted_messages} messages."
        )

    def _apply_tags(self, value: str) -> None:
        conversation_id = self._current_conversation_id()
        if not conversation_id:
            return
        tags = [tag.strip() for tag in value.split(",") if tag.strip()]
        self.db.set_tags(conversation_id, tags)
        self.db.update_fts(conversation_id)
        self.db.commit()
        self.load_conversations(self.search_query)

    def _retitle_current(self, title: str) -> None:
        conversation_id = self._current_conversation_id()
        if not conversation_id:
            return
        messages = self.db.get_messages(conversation_id)
        payloads = [
            {
                "author_role": msg.get("author_role") or "unknown",
                "content": msg.get("content") or "",
                "created_at": msg.get("created_at") or 0,
            }
            for msg in messages
        ]
        conversation = self.db.get_conversation(conversation_id)
        created_at = conversation.get("created_at") or 0
        updated_at = conversation.get("updated_at") or 0
        content_hash = fingerprint_conversation(title, created_at, updated_at, payloads)
        self.db.conn.execute(
            "UPDATE conversations SET title = ?, content_hash = ? WHERE id = ?",
            (title, content_hash, conversation_id),
        )
        self.db.update_fts(conversation_id)
        self.db.commit()
        self.load_conversations(self.search_query)

    def _notify(self, message: str) -> None:
        try:
            super().notify(message)
        except AttributeError:
            self.preview.write(f"\n{message}")

    def _load_db_history(self) -> list[Path]:
        history = load_db_history(self.config.paths)
        current = self.config.paths.db_path
        merged = [current] + history
        return self._dedupe_paths(merged)

    def _save_db_history(self) -> None:
        save_db_history(self.config.paths, self.db_history)

    def _dedupe_paths(self, paths: list[Path]) -> list[Path]:
        seen: set[str] = set()
        result: list[Path] = []
        for path in paths:
            key = str(path.resolve()) if path.exists() else str(path)
            if key in seen:
                continue
            seen.add(key)
            result.append(path)
        return result

    def _switch_db(self, new_path: Path) -> None:
        path = new_path.expanduser()
        if not path.is_absolute():
            path = (self.config.paths.data_dir / path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.db.close()
        except Exception:  # noqa: BLE001
            pass
        updated_paths = self.config.paths.__class__(
            config_dir=self.config.paths.config_dir,
            data_dir=self.config.paths.data_dir,
            state_dir=self.config.paths.state_dir,
            cache_dir=self.config.paths.cache_dir,
            config_file=self.config.paths.config_file,
            db_path=path,
            log_file=self.config.paths.log_file,
            db_history_file=self.config.paths.db_history_file,
        )
        self.config = self.config.__class__(
            paths=updated_paths,
            date_format=self.config.date_format,
            chat_url_base=self.config.chat_url_base,
        )
        save_config(self.config)
        self.db = Database(self.config.paths.db_path)
        self.db.initialize()
        self.importer = Importer(self.db)
        self.db_history = self._dedupe_paths([self.config.paths.db_path] + self.db_history)
        self._save_db_history()
        self.load_conversations(self.search_query)
        self._notify(f"Switched database: {self.config.paths.db_path}")

    def _move_table(self, delta: int) -> None:
        if self.table.row_count == 0:
            return
        try:
            if delta > 0:
                self.table.cursor_down()
            else:
                self.table.cursor_up()
            return
        except AttributeError:
            pass
        row = self.table.cursor_row if self.table.cursor_row is not None else 0
        new_row = max(0, min(row + delta, self.table.row_count - 1))
        self.table.cursor_coordinate = (new_row, 0)

    def _apply_sort_command(self, parts: list[str]) -> None:
        if len(parts) < 2:
            self._notify("Usage: sort <updated|created|title|messages> [asc|desc]")
            return
        field = parts[1].lower()
        if field not in {"updated", "created", "title", "messages"}:
            self._notify("Sort fields: updated, created, title, messages")
            return
        order = parts[2].lower() if len(parts) > 2 else self.sort_order
        if order not in {"asc", "desc"}:
            self._notify("Sort order must be asc or desc")
            return
        self.sort_by = field
        self.sort_order = order
        self.load_conversations(self.search_query)
        self._notify(f"Sort: {self.sort_by} {self.sort_order}")

    def _apply_filter_command(self, value: str, parts: list[str]) -> None:
        if len(parts) < 2:
            self._notify(
                "Usage: filter <field> <value> | filter clear. Fields: title, tags, updated, "
                "created, messages, source, id"
            )
            return
        field = parts[1].lower()
        if field in {"clear", "reset"}:
            self._clear_filters()
            return
        aliases = {"tag": "tags", "date": "updated"}
        field = aliases.get(field, field)
        if field not in {"title", "tags", "updated", "created", "messages", "source", "id"}:
            self._notify("Filter fields: title, tags, updated, created, messages, source, id")
            return
        remainder = value.split(None, 2)
        raw = remainder[2] if len(remainder) > 2 else ""
        ok, message = self._apply_filter_field(field, raw)
        if message:
            self._notify(message)

    def _clear_filters(self) -> None:
        self.filter_tags = []
        self.filter_title = None
        self.filter_source = None
        self.filter_id = None
        self.filter_updated_from = None
        self.filter_updated_to = None
        self.filter_created_from = None
        self.filter_created_to = None
        self.filter_message_min = None
        self.filter_message_max = None
        self.load_conversations(self.search_query)
        self._notify("Filters cleared")

    def _clear_filter_field(self, field: str) -> None:
        if field == "tags":
            self.filter_tags = []
        elif field == "title":
            self.filter_title = None
        elif field == "source":
            self.filter_source = None
        elif field == "id":
            self.filter_id = None
        elif field == "updated":
            self.filter_updated_from = None
            self.filter_updated_to = None
        elif field == "created":
            self.filter_created_from = None
            self.filter_created_to = None
        elif field == "messages":
            self.filter_message_min = None
            self.filter_message_max = None

    def _apply_filter_field(self, field: str, raw: str) -> tuple[bool, str]:
        value = raw.strip()
        if value == "":
            self._clear_filter_field(field)
            self.load_conversations(self.search_query)
            return True, f"Cleared filter: {field}"

        if field in {"title", "source", "id"}:
            cleaned = self._strip_quotes(value)
            if field == "title":
                self.filter_title = cleaned
            elif field == "source":
                self.filter_source = cleaned
            else:
                self.filter_id = cleaned
        elif field == "tags":
            tags = [tag.strip() for tag in value.split(",") if tag.strip()]
            self.filter_tags = tags
        elif field in {"updated", "created"}:
            start_raw, end_raw = self._split_range(value)
            start = self._parse_date(start_raw, end=False) if start_raw else None
            end = self._parse_date(end_raw, end=True) if end_raw else None
            if (start_raw and start is None) or (end_raw and end is None):
                return False, "Invalid date format. Use YYYY-MM-DD"
            if field == "updated":
                self.filter_updated_from = start
                self.filter_updated_to = end
            else:
                self.filter_created_from = start
                self.filter_created_to = end
        elif field == "messages":
            start_raw, end_raw = self._split_range(value)
            start, end = self._parse_int_range(start_raw, end_raw)
            if (start_raw and start is None) or (end_raw and end is None):
                return False, "Invalid range. Use \"5 20\" or \"5..20\""
            self.filter_message_min = start
            self.filter_message_max = end
        else:
            return False, "Unknown filter field."

        self.load_conversations(self.search_query)
        return True, f"Filter applied: {field}"

    def _strip_quotes(self, value: str) -> str:
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            return value[1:-1]
        return value

    def _split_range(self, value: str) -> tuple[str | None, str | None]:
        text = value.strip()
        if ".." in text:
            left, right = text.split("..", 1)
            return (left.strip() or None, right.strip() or None)
        parts = text.split()
        if len(parts) == 1:
            return parts[0], None
        return parts[0], parts[1]

    def _parse_int_range(
        self, start_raw: str | None, end_raw: str | None
    ) -> tuple[int | None, int | None]:
        start = None
        end = None
        if start_raw:
            try:
                start = int(start_raw)
            except ValueError:
                start = None
        if end_raw:
            try:
                end = int(end_raw)
            except ValueError:
                end = None
        return start, end

    def _parse_date(self, value: str, end: bool) -> int | None:
        try:
            dt = datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            return None
        if end:
            dt = dt.replace(hour=23, minute=59, second=59)
        return int(dt.timestamp())

    def _format_date_only(self, ts: int | None) -> str:
        if not ts:
            return ""
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")

    def _format_range(self, start: int | None, end: int | None) -> str:
        if start is None and end is None:
            return ""
        if start is not None and end is not None:
            return f"{start}..{end}"
        if start is not None:
            return f"{start}.."
        return f"..{end}"

    def _format_date_range(self, start: int | None, end: int | None) -> str:
        if start is None and end is None:
            return ""
        if start is not None and end is not None:
            return f"{self._format_date_only(start)}..{self._format_date_only(end)}"
        if start is not None:
            return f"{self._format_date_only(start)}.."
        return f"..{self._format_date_only(end)}"

    def _filter_display(self, field: str) -> str:
        if field == "tags":
            return ",".join(self.filter_tags)
        if field == "title":
            return self.filter_title or ""
        if field == "source":
            return self.filter_source or ""
        if field == "id":
            return self.filter_id or ""
        if field == "updated":
            return self._format_date_range(self.filter_updated_from, self.filter_updated_to)
        if field == "created":
            return self._format_date_range(self.filter_created_from, self.filter_created_to)
        if field == "messages":
            return self._format_range(self.filter_message_min, self.filter_message_max)
        return ""

    def _update_title(self) -> None:
        parts = [f"sort={self.sort_by} {self.sort_order}"]
        if self.filter_tags:
            parts.append(f"tags={','.join(self.filter_tags)}")
        if self.filter_title:
            parts.append(f"title~{self.filter_title}")
        if self.filter_source:
            parts.append(f"source~{self.filter_source}")
        if self.filter_id:
            parts.append(f"id~{self.filter_id}")
        if self.filter_updated_from or self.filter_updated_to:
            parts.append(
                f"updated={self._format_date_range(self.filter_updated_from, self.filter_updated_to)}"
            )
        if self.filter_created_from or self.filter_created_to:
            parts.append(
                f"created={self._format_date_range(self.filter_created_from, self.filter_created_to)}"
            )
        if self.filter_message_min is not None or self.filter_message_max is not None:
            parts.append(
                f"messages={self._format_range(self.filter_message_min, self.filter_message_max)}"
            )
        self.title = self.config.paths.db_path.stem or "threadindex"
        self.sub_title = " | ".join(parts)

    def _current_conversation_id(self) -> str | None:
        if self.table.row_count == 0:
            return None
        idx = self.table.cursor_row
        if idx is None or idx < 0 or idx >= len(self.row_ids):
            return None
        return self.row_ids[idx]

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key:
            self.show_preview(event.row_key.value)


def run_tui() -> None:
    app = ThreadIndexApp()
    app.run()
