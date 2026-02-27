from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from platformdirs import user_cache_path, user_config_path, user_data_path, user_state_path

APP_NAME = "threadindex"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M"
DEFAULT_CHAT_URL_BASE = "https://chatgpt.com/c/"
DEFAULT_CHAT_URL_BASES = {
    "chatgpt": "https://chatgpt.com/c/",
    "deepseek": "https://chat.deepseek.com/a/chat/s/",
}


@dataclass(frozen=True)
class Paths:
    config_dir: Path
    data_dir: Path
    state_dir: Path
    cache_dir: Path
    config_file: Path
    db_path: Path
    log_file: Path
    db_history_file: Path


@dataclass(frozen=True)
class Config:
    paths: Paths
    date_format: str
    chat_url_base: str
    chat_url_bases: dict[str, str]


def get_paths() -> Paths:
    config_base = os.environ.get("XDG_CONFIG_HOME")
    data_base = os.environ.get("XDG_DATA_HOME")
    state_base = os.environ.get("XDG_STATE_HOME")
    cache_base = os.environ.get("XDG_CACHE_HOME")

    config_dir = (
        Path(config_base) / APP_NAME
        if config_base
        else user_config_path(APP_NAME, appauthor=False)
    )
    data_dir = (
        Path(data_base) / APP_NAME
        if data_base
        else user_data_path(APP_NAME, appauthor=False)
    )
    state_dir = (
        Path(state_base) / APP_NAME
        if state_base
        else user_state_path(APP_NAME, appauthor=False)
    )
    cache_dir = (
        Path(cache_base) / APP_NAME
        if cache_base
        else user_cache_path(APP_NAME, appauthor=False)
    )

    config_file = config_dir / "config.toml"
    db_path = data_dir / "threadindex.db"
    log_file = state_dir / "threadindex.log"
    db_history_file = state_dir / "dbs.json"

    return Paths(
        config_dir=config_dir,
        data_dir=data_dir,
        state_dir=state_dir,
        cache_dir=cache_dir,
        config_file=config_file,
        db_path=db_path,
        log_file=log_file,
        db_history_file=db_history_file,
    )


def ensure_dirs(paths: Paths) -> None:
    paths.config_dir.mkdir(parents=True, exist_ok=True)
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    paths.state_dir.mkdir(parents=True, exist_ok=True)
    paths.cache_dir.mkdir(parents=True, exist_ok=True)


def _write_default_config(paths: Paths) -> None:
    content = (
        "[paths]\n"
        f"db_path = \"{paths.db_path.as_posix()}\"\n"
        "\n"
        "[ui]\n"
        f"date_format = \"{DEFAULT_DATE_FORMAT}\"\n"
        "\n"
        "[links]\n"
        f"chat_url_base = \"{DEFAULT_CHAT_URL_BASE}\"\n"
        "\n"
        "[links.chat_url_base_by_source]\n"
        f"chatgpt = \"{DEFAULT_CHAT_URL_BASES['chatgpt']}\"\n"
        f"deepseek = \"{DEFAULT_CHAT_URL_BASES['deepseek']}\"\n"
    )
    paths.config_file.write_text(content, encoding="utf-8")


def load_config() -> Config:
    paths = get_paths()
    ensure_dirs(paths)

    if not paths.config_file.exists():
        _write_default_config(paths)

    date_format = DEFAULT_DATE_FORMAT
    chat_url_base = DEFAULT_CHAT_URL_BASE
    chat_url_bases = dict(DEFAULT_CHAT_URL_BASES)
    db_path = paths.db_path

    try:
        raw = tomllib.loads(paths.config_file.read_text(encoding="utf-8"))
        ui = raw.get("ui", {})
        date_format = ui.get("date_format", DEFAULT_DATE_FORMAT)
        path_cfg = raw.get("paths", {})
        if "db_path" in path_cfg:
            candidate = Path(path_cfg["db_path"]).expanduser()
            if not candidate.is_absolute():
                candidate = paths.data_dir / candidate
            db_path = candidate
        links = raw.get("links", {})
        chat_url_base = links.get("chat_url_base", DEFAULT_CHAT_URL_BASE)
        raw_map = links.get("chat_url_base_by_source", {})
        if isinstance(raw_map, dict):
            for key, value in raw_map.items():
                if isinstance(key, str) and isinstance(value, str) and value.strip():
                    chat_url_bases[key.lower()] = value
    except (OSError, tomllib.TOMLDecodeError):
        pass

    updated_paths = Paths(
        config_dir=paths.config_dir,
        data_dir=paths.data_dir,
        state_dir=paths.state_dir,
        cache_dir=paths.cache_dir,
        config_file=paths.config_file,
        db_path=db_path,
        log_file=paths.log_file,
        db_history_file=paths.db_history_file,
    )

    return Config(
        paths=updated_paths,
        date_format=date_format,
        chat_url_base=chat_url_base,
        chat_url_bases=chat_url_bases,
    )


def save_config(config: Config) -> None:
    ensure_dirs(config.paths)
    source_bases = dict(DEFAULT_CHAT_URL_BASES)
    source_bases.update({key.lower(): value for key, value in config.chat_url_bases.items()})
    mapping_lines = "".join(
        f"{key} = \"{value}\"\n" for key, value in sorted(source_bases.items(), key=lambda i: i[0])
    )
    content = (
        "[paths]\n"
        f"db_path = \"{config.paths.db_path.as_posix()}\"\n"
        "\n"
        "[ui]\n"
        f"date_format = \"{config.date_format}\"\n"
        "\n"
        "[links]\n"
        f"chat_url_base = \"{config.chat_url_base}\"\n"
        "\n"
        "[links.chat_url_base_by_source]\n"
        f"{mapping_lines}"
    )
    config.paths.config_file.write_text(content, encoding="utf-8")


def load_db_history(paths: Paths) -> list[Path]:
    try:
        raw = json.loads(paths.db_history_file.read_text(encoding="utf-8"))
    except OSError:
        return []
    except json.JSONDecodeError:
        return []
    if not isinstance(raw, list):
        return []
    dbs: list[Path] = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            dbs.append(Path(item).expanduser())
    return dbs


def save_db_history(paths: Paths, db_paths: list[Path]) -> None:
    ensure_dirs(paths)
    payload = [str(path) for path in db_paths]
    paths.db_history_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
