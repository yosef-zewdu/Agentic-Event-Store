# upcasting package
# Importing upcasters triggers registration against the singleton registry.
from src.upcasting import upcasters as _upcasters  # noqa: F401
from src.upcasting.registry import registry

__all__ = ["registry"]
