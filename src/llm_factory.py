"""
src/llm_factory.py — Unified LLM provider factory.

Supports: openrouter, groq, ollama, gemini, huggingface
Provides:
  - get_llm_config()   → raw config dict (provider, model, api_key, base_url)
  - get_llm()          → LangChain ChatModel (for LangChain-based usage)
  - get_async_client() → AsyncOpenAI client (for agents using _call_llm directly)
  - compute_cost()     → USD cost from token counts + model name
  - MODEL_PRICING      → pricing table (USD per million tokens)
"""
from __future__ import annotations

import logging
import os
from typing import Any

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pricing table — USD per million tokens
# Add new models here; unknown models default to $0
# ---------------------------------------------------------------------------
MODEL_PRICING: dict[str, dict[str, float]] = {
    # OpenRouter / Google
    "google/gemini-3.1-pro-preview":        {"input": 1.0,  "output": 12.0},
    "google/gemini-flash-1.5":              {"input": 0.075,"output": 0.30},
    # OpenRouter free tier
    "arcee-ai/trinity-large-preview:free":  {"input": 0.0,  "output": 0.0},
    # Groq (free tier, approximate)
    "llama-3.2-90b-vision-preview":         {"input": 0.0,  "output": 0.0},
    "llama-3.3-70b-versatile":              {"input": 0.0,  "output": 0.0},
    # OpenAI
    "openai/gpt-4o":                        {"input": 2.5,  "output": 10.0},
    "openai/gpt-4o-mini":                   {"input": 0.15, "output": 0.60},
    "openai/gpt-3.5-turbo":                 {"input": 0.50, "output": 1.50},
    # Anthropic (via OpenRouter)
    "anthropic/claude-3-haiku":             {"input": 0.25, "output": 1.25},
    "anthropic/claude-3.5-sonnet":          {"input": 3.0,  "output": 15.0},
    # Ollama / local — always free
    "deepseek-coder":                       {"input": 0.0,  "output": 0.0},
}


def compute_cost(model: str, tok_in: int, tok_out: int) -> float:
    """Return USD cost for a given model and token counts."""
    pricing = MODEL_PRICING.get(model, {"input": 0.0, "output": 0.0})
    return (tok_in * pricing["input"] + tok_out * pricing["output"]) / 1_000_000


def get_llm_config() -> dict[str, Any]:
    """
    Returns LLM configuration from environment variables.

    Returns dict with keys: provider, model, api_key, base_url, available
    """
    provider = os.getenv("LLM_PROVIDER", "openrouter").lower()

    config: dict[str, Any] = {
        "provider": provider,
        "model": None,
        "api_key": None,
        "available": False,
        "base_url": None,
    }

    if provider == "openrouter":
        config["model"] = os.getenv("OPENROUTER_MODEL", "arcee-ai/trinity-large-preview:free")
        config["api_key"] = os.getenv("OPENROUTER_API_KEY")
        config["available"] = bool(config["api_key"])
        config["base_url"] = "https://openrouter.ai/api/v1"

    elif provider == "groq":
        config["model"] = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
        config["api_key"] = os.getenv("GROQ_API_KEY")
        config["available"] = bool(config["api_key"])
        config["base_url"] = "https://api.groq.com/openai/v1"

    elif provider == "ollama":
        config["model"] = os.getenv("OLLAMA_MODEL", "deepseek-coder")
        config["base_url"] = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        config["api_key"] = "ollama"
        config["available"] = True

    elif provider == "gemini":
        config["model"] = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        config["api_key"] = os.getenv("GOOGLE_API_KEY")
        config["available"] = bool(config["api_key"])
        # Gemini via OpenAI-compatible endpoint
        config["base_url"] = "https://generativelanguage.googleapis.com/v1beta/openai/"

    elif provider == "huggingface":
        config["model"] = os.getenv("HF_MODEL", "Qwen/Qwen2.5-7B-Instruct")
        config["api_key"] = os.getenv("HF_TOKEN")
        config["available"] = bool(config["api_key"])
        config["base_url"] = "https://api-inference.huggingface.co/v1"

    else:
        logger.warning("Unknown LLM provider: %s", provider)

    return config


def get_async_client():
    """
    Returns an AsyncOpenAI client configured for the active provider.
    Used by BaseApexAgent._call_llm() — all providers expose an OpenAI-compatible API.

    Returns:
        (AsyncOpenAI client, model_name str)
    """
    from openai import AsyncOpenAI

    cfg = get_llm_config()

    if not cfg["available"]:
        raise ValueError(
            f"LLM provider '{cfg['provider']}' is not available. "
            f"Check your API key environment variable."
        )

    if cfg["provider"] == "gemini":
        # Native Gemini SDK path — wrap via openai-compatible endpoint
        client = AsyncOpenAI(
            api_key=cfg["api_key"],
            base_url=cfg["base_url"],
        )
    elif cfg["provider"] == "huggingface":
        client = AsyncOpenAI(
            api_key=cfg["api_key"],
            base_url=cfg["base_url"],
        )
    else:
        # openrouter, groq, ollama all use standard OpenAI-compatible base_url
        client = AsyncOpenAI(
            api_key=cfg["api_key"],
            base_url=cfg["base_url"],
        )

    return client, cfg["model"]


def get_llm(provider: str | None = None, model: str | None = None, api_key: str | None = None):
    """
    Returns a LangChain ChatModel for the active provider.
    Use this when integrating with LangChain chains/tools directly.

    Args:
        provider: override LLM_PROVIDER env var
        model:    override model env var
        api_key:  override API key env var

    Returns:
        BaseChatModel instance

    Raises:
        ValueError: unsupported provider or missing API key
    """
    from langchain_openai import ChatOpenAI

    provider = provider or os.getenv("LLM_PROVIDER", "openrouter").lower()

    if provider in ("openrouter", "groq", "ollama", "huggingface"):
        cfg = get_llm_config()
        resolved_model = model or cfg["model"]
        resolved_key = api_key or cfg["api_key"]
        resolved_url = cfg["base_url"]

        if provider != "ollama" and not resolved_key:
            raise ValueError(f"{provider.upper()}_API_KEY not set in environment")

        return ChatOpenAI(
            base_url=resolved_url,
            openai_api_key=resolved_key or "none",
            model=resolved_model,
            temperature=0,
        )

    elif provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI
        resolved_model = model or os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        resolved_key = api_key or os.getenv("GOOGLE_API_KEY")
        if not resolved_key:
            raise ValueError("GOOGLE_API_KEY not set in environment")
        return ChatGoogleGenerativeAI(
            model=resolved_model,
            google_api_key=resolved_key,
            temperature=0,
        )

    else:
        raise ValueError(
            f"Unsupported LLM provider: {provider}. "
            f"Supported: openrouter, groq, ollama, gemini, huggingface"
        )
