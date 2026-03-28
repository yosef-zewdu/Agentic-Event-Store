"""Custom exceptions for The Ledger — Agentic Event Store."""


class OptimisticConcurrencyError(Exception):
    def __init__(
        self,
        stream_id: str,
        expected_version: int,
        actual_version: int,
        suggested_action: str = "reload_stream_and_retry",
    ):
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        self.expected = expected_version   # alias for test compatibility
        self.actual = actual_version       # alias for test compatibility
        self.suggested_action = suggested_action
        super().__init__(
            f"Stream {stream_id} at version {actual_version}, expected {expected_version}"
        )


class DomainError(Exception):
    def __init__(self, message: str, context: dict | None = None):
        self.context = context or {}
        super().__init__(message)


class StreamNotFoundError(Exception):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream not found: {stream_id}")


class RetryBudgetExhausted(Exception):
    """Raised when OCC retry budget is exhausted after max_retries attempts."""
    def __init__(self, stream_id: str, attempts: int):
        self.stream_id = stream_id
        self.attempts = attempts
        super().__init__(
            f"OCC retry budget ({attempts}) exhausted for stream {stream_id}. "
            "Investigate whether two agents are competing for the same stream."
        )
