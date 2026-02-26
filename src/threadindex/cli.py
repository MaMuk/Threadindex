from __future__ import annotations

from pathlib import Path

import typer

from .config import load_config, load_db_history, save_config, save_db_history
from .db import Database
from .importer import Importer
from .tui import run_tui

app = typer.Typer(add_completion=False)
doctor_app = typer.Typer(add_completion=False)
app.add_typer(doctor_app, name="doctor")


def _open_db() -> Database:
    config = load_config()
    db = Database(config.paths.db_path)
    db.initialize()
    return db


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        run_tui()


@app.command("import")
def import_data(path: Path, dry_run: bool = typer.Option(False, help="Preview changes")) -> None:
    """Import an export zip/dir/file into the local database."""
    db = _open_db()
    importer = Importer(db)
    result = importer.import_path(path, dry_run=dry_run)
    if dry_run:
        typer.echo("Dry run complete. No changes were written.")
    typer.echo(
        f"Inserted conversations: {result.inserted_conversations}, "
        f"Updated: {result.updated_conversations}, "
        f"Inserted messages: {result.inserted_messages}, "
        f"Updated messages: {result.updated_messages}, "
        f"Skipped: {result.skipped}"
    )
    typer.echo(f"Source hash: {result.source_hash}")


@app.command()
def reindex() -> None:
    """Rebuild the full-text search index."""
    db = _open_db()
    count = db.rebuild_fts()
    db.commit()
    typer.echo(f"Reindexed {count} conversations.")


@doctor_app.callback(invoke_without_command=True)
def doctor() -> None:
    """Print configuration paths and database status."""
    config = load_config()
    db = Database(config.paths.db_path)
    db.initialize()
    info = db.doctor_info()
    typer.echo("Paths:")
    typer.echo(f"  Config: {config.paths.config_file}")
    typer.echo(f"  Data:   {config.paths.data_dir}")
    typer.echo(f"  State:  {config.paths.state_dir}")
    typer.echo(f"  Cache:  {config.paths.cache_dir}")
    typer.echo(f"  DB:     {config.paths.db_path}")
    typer.echo("Database:")
    typer.echo(f"  Conversations: {info['conversations']}")
    typer.echo(f"  Messages:      {info['messages']}")
    typer.echo(f"  FTS rows:      {info['fts_rows']}")


@doctor_app.command("db-set")
def doctor_db_set(database: str) -> None:
    """Set the default database path for tindex."""
    config = load_config()
    candidate = Path(database).expanduser()
    if not candidate.is_absolute():
        candidate = config.paths.data_dir / candidate
    updated_paths = config.paths.__class__(
        config_dir=config.paths.config_dir,
        data_dir=config.paths.data_dir,
        state_dir=config.paths.state_dir,
        cache_dir=config.paths.cache_dir,
        config_file=config.paths.config_file,
        db_path=candidate,
        log_file=config.paths.log_file,
        db_history_file=config.paths.db_history_file,
    )
    updated = config.__class__(
        paths=updated_paths,
        date_format=config.date_format,
        chat_url_base=config.chat_url_base,
    )
    save_config(updated)
    history = load_db_history(config.paths)
    history = [candidate] + [path for path in history if path != candidate]
    save_db_history(config.paths, history)
    typer.echo(f"Default database set to: {candidate}")


@app.command()
def dump(conversation_id: str, output: Path | None = None) -> None:
    """Print a conversation transcript."""
    db = _open_db()
    conversation = db.get_conversation(conversation_id)
    if not conversation:
        raise typer.Exit(code=1)
    messages = db.get_messages(conversation_id)
    lines: list[str] = []
    lines.append(f"Title: {conversation['title']}")
    lines.append(f"ID: {conversation['id']}")
    lines.append("")
    for message in messages:
        role = message["author_role"]
        content = message["content"] or ""
        lines.append(f"[{role}] {content}")
        lines.append("")
    transcript = "\n".join(lines).strip()
    if output:
        output.write_text(transcript, encoding="utf-8")
    else:
        typer.echo(transcript)


if __name__ == "__main__":
    app()
