
import logging
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from pathlib import Path

from dotenv import load_dotenv
from fastmcp import FastMCP

# Load .env from the project root so DATABASE_URL is available when the
# server is started via `fastmcp run` (which doesn't inherit shell exports).
# load_dotenv(Path(__file__).parent.parent.parent / ".env")

from src.event_store import EventStore
# from src.upcasting.registry import registry as upcaster_registry

# logger = logging.getLogger(__name__)

    
# ---------------------------------------------------------------------------
# Shared EventStore instance (populated during startup)
# ---------------------------------------------------------------------------
_store: EventStore | None = None

def set_store(store: EventStore | None) -> None:
    global _store
    _store = store
    
def get_store() -> EventStore:
    """Return the shared EventStore instance.

    Raises RuntimeError if called before the server has started.
    """
    if _store is None:
        raise RuntimeError(
            "EventStore is not initialised. "
            "Ensure the MCP server lifecycle has started before calling get_store()."
        )
    return _store


