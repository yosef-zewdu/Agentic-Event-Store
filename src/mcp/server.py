"""
MCP server entry point for The Ledger.

Initialises the FastMCP server, wires the database connection pool,
and manages startup/shutdown lifecycle.

Tools and resources are registered in tools.py and resources.py respectively.
"""

from fastmcp import FastMCP
from src.mcp.tools import register_tools
from src.mcp.resources import register_resources
from src.mcp.utils import set_store

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from pathlib import Path
import os
import logging
from src.event_store import EventStore
from src.upcasting.registry import registry as upcaster_registry
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


# Load .env from the project root so DATABASE_URL is available when the
# server is started via `fastmcp run` (which doesn't inherit shell exports).
load_dotenv(Path(__file__).parent.parent.parent / ".env")

# ---------------------------------------------------------------------------
# Lifespan — create pool on startup, close on shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(server: FastMCP) -> AsyncIterator[dict]:
    """Manage the asyncpg connection pool for the lifetime of the server."""
    # global _store

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Set it to a valid asyncpg-compatible PostgreSQL DSN, e.g. "
            "postgresql://user:password@host/dbname"
        )

    logger.info("Connecting to database…")
    store = EventStore(db_url=db_url, upcaster_registry=upcaster_registry)
    await store.connect()
    set_store(store)
    logger.info("EventStore ready.")

    # Ensure all projection tables exist on startup
    from src.projections.application_summary import ApplicationSummaryProjection
    from src.projections.agent_performance import AgentPerformanceLedgerProjection
    from src.projections.compliance_audit import ComplianceAuditViewProjection
    async with store._pool.acquire() as conn:
        await ApplicationSummaryProjection().ensure_table_exists(conn)
        await AgentPerformanceLedgerProjection().ensure_table_exists(conn)
        await ComplianceAuditViewProjection().ensure_table_exists(conn)
    logger.info("Projection tables ready.")

    try:
        yield {"store": store}
    finally:
        logger.info("Closing database connection pool…")
        await store.close()
        set_store(None)
        logger.info("Database connection pool closed.")


# Create one shared MCP server
mcp = FastMCP("The Ledger", lifespan=_lifespan)

# Register tools and resources on the same server instance
register_tools(mcp)
register_resources(mcp)

if __name__ == "__main__":
    mcp.run()