"""
tests/conftest.py — shared fixtures for The Ledger test suite.

Requires a running PostgreSQL instance. DATABASE_URL is read from the
environment. The fixture creates the test database automatically if it
does not exist.
"""

from __future__ import annotations

import os
import random
import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import asyncpg
import pytest
import pytest_asyncio
from faker import Faker

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env from the project root so DATABASE_URL is available without
# having to manually export it before running pytest.
load_dotenv(Path(__file__).parent.parent / ".env")

random.seed(42)
Faker.seed(42)

# ---------------------------------------------------------------------------
# Database URL resolution
# ---------------------------------------------------------------------------
_BASE_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
if not _BASE_URL:
    raise RuntimeError(
        "Set DATABASE_URL or TEST_DATABASE_URL in your .env or environment before running tests."
    )

_parsed = urlparse(_BASE_URL)
_TEST_DB_NAME = _parsed.path.lstrip("/") + "_test"
DATABASE_URL = urlunparse(_parsed._replace(path=f"/{_TEST_DB_NAME}"))
_ADMIN_URL = urlunparse(_parsed._replace(path="/postgres"))


async def _ensure_test_db() -> None:
    """Create the test database if it doesn't already exist."""
    conn = await asyncpg.connect(_ADMIN_URL)
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", _TEST_DB_NAME
        )
        if not exists:
            await conn.execute(f'CREATE DATABASE "{_TEST_DB_NAME}"')
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Starter-compat fixtures (kept so existing starter tests don't break)
# ---------------------------------------------------------------------------

@pytest.fixture
def db_url():
    return DATABASE_URL


@pytest.fixture
def sample_companies():
    from datagen.company_generator import generate_companies
    return generate_companies(10)


@pytest.fixture
def event_store_class():
    """Returns the EventStore class."""
    from src.event_store import EventStore
    return EventStore


# ---------------------------------------------------------------------------
# Async DB fixtures
# ---------------------------------------------------------------------------

from src.event_store import EventStore  # noqa: E402


@pytest_asyncio.fixture
async def db_pool():
    """Function-scoped asyncpg pool. Creates the test DB and applies schema on first run."""
    await _ensure_test_db()

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)

    schema_path = Path(__file__).parent.parent / "src" / "schema.sql"
    async with pool.acquire() as conn:
        try:
            await conn.execute(schema_path.read_text())
        except Exception:
            pass  # schema already applied (types/tables exist)

    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def store(db_pool):
    """
    Function-scoped EventStore backed by PostgreSQL.
    Truncates all tables before each test for full isolation.
    """
    async with db_pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints "
            "RESTART IDENTITY CASCADE"
        )
    es = EventStore(DATABASE_URL)
    es._pool = db_pool  # reuse the session pool — no extra connect() needed
    yield es
    # pool is session-scoped; don't close it here
