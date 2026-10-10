"""Configuration reads secret files without printing their content or DSNs."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

from psycopg.conninfo import make_conninfo


def secret_file(name: str) -> str:
    path = os.environ.get(name)
    if not path:
        raise ValueError(f"Missing {name}")
    value = Path(path).read_text(encoding="utf-8").strip()
    if len(value) < 16 or len(value) > 4096 or "\n" in value:
        raise ValueError(f"Invalid {name}")
    return value


@dataclass(frozen=True)
class Settings:
    api_url: str
    token: str = field(repr=False)
    database_dsn: str = field(repr=False)
    redis_url: str = field(repr=False)
    poll_seconds: float = 1.0

    @classmethod
    def load(cls) -> Settings:
        token = secret_file("ADF_ORCHESTRATOR_TOKEN_FILE")
        password = secret_file("ADF_DB_PASSWORD_FILE")
        redis_password = secret_file("ADF_REDIS_PASSWORD_FILE")
        database = make_conninfo(
            os.environ["DATABASE_URL"],
            password=password,
            connect_timeout=5,
            keepalives_idle=10,
            keepalives_interval=5,
            keepalives_count=3,
            tcp_user_timeout=15000,
        )
        redis = urlparse(os.environ["ADF_REDIS_URL"])
        if redis.scheme not in {"redis", "rediss"} or not redis.hostname:
            raise ValueError("Invalid ADF_REDIS_URL")
        userinfo = quote(redis.username or "default", safe="") + ":" + quote(redis_password, safe="")
        redis_uri = urlunparse(redis._replace(netloc=f"{userinfo}@{redis.hostname}:{redis.port or 6379}"))
        api = os.environ.get("ADF_API_URL", "http://api:8080").rstrip("/")
        parsed_api = urlparse(api)
        if (
            parsed_api.scheme not in {"http", "https"}
            or parsed_api.username
            or parsed_api.password
            or not parsed_api.hostname
        ):
            raise ValueError("Invalid ADF_API_URL")
        return cls(api, token, database, redis_uri)
