# -*- coding: utf-8 -*-
"""
src/agents/base_agent.py -- BaseApexAgent base class and module-level constants
"""
from __future__ import annotations
import asyncio, hashlib, json, re, time
from abc import ABC, abstractmethod
from datetime import datetime
from uuid import uuid4
from openai import AsyncOpenAI
from langgraph.graph import StateGraph, END
import openai
from src.llm_factory import compute_cost

LANGGRAPH_VERSION = "1.0.0"
MAX_OCC_RETRIES = 5
SCREENING_MODEL_VERSION = "fraud-v1.0"
REGULATION_SET_VERSION = "2026-Q1"


def _compute_cost(model: str, tok_in: int, tok_out: int) -> float:
    """Delegate to llm_factory.compute_cost — single source of truth."""
    return compute_cost(model, tok_in, tok_out)


class BaseApexAgent(ABC):
    def __init__(self, agent_id, agent_type, store, registry, client: AsyncOpenAI,
                 model="arcee-ai/trinity-large-preview:free"):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.store = store
        self.registry = registry
        self.client = client
        self.model = model
        self.session_id = None
        self.application_id = None
        self._session_stream = None
        self._t0 = None
        self._seq = 0
        self._llm_calls = 0
        self._tokens = 0
        self._cost = 0.0
        self._graph = None
        self._llm_errors: list[str] = []
        self._tokens_input = 0
        self._tokens_output = 0
        self._fail_at_node: str | None = None  # For testing crash recovery

    @abstractmethod
    def build_graph(self): raise NotImplementedError

    async def process_application(self, application_id: str, resume_session_id: str | None = None) -> None:
        if not self._graph:
            self._graph = self.build_graph()
        self.application_id = application_id
        if resume_session_id:
            self.session_id = resume_session_id
            self._session_stream = f"session-{resume_session_id}"
            events = await self.store.load_stream(self._session_stream)
            if not events:
                raise ValueError(f"Session {resume_session_id} not found")
            # Check session is in a resumable state — not already completed successfully
            terminal_types = {"AgentSessionCompleted"}
            terminal = next((e for e in events if e.event_type in terminal_types), None)
            if terminal:
                raise ValueError(
                    f"Session {resume_session_id} already completed successfully "
                    f"and cannot be resumed"
                )
            started = next((e for e in events if e.event_type == "AgentSessionStarted"), None)
            if started:
                self._t0 = time.time()
                self._seq = len([e for e in events if e.event_type == "AgentNodeExecuted"])
            else:
                raise ValueError(f"Session {resume_session_id} has no AgentSessionStarted event")
        else:
            self.session_id = f"sess-{self.agent_type[:3]}-{uuid4().hex[:8]}"
            self._session_stream = f"session-{self.session_id}"
            self._t0 = time.time()
            self._seq = 0
        self._llm_calls = self._tokens = 0
        self._cost = 0.0
        self._llm_errors = []
        self._tokens_input = 0
        self._tokens_output = 0
        if not resume_session_id:
            await self._start_session(application_id)
        try:
            result = await asyncio.wait_for(
                self._graph.ainvoke(self._initial_state(application_id)),
                timeout=300.0,  # 5-minute hard cap per agent run
            )
            await self._complete_session(result)
        except asyncio.TimeoutError:
            await self._fail_session("TimeoutError", "Agent run exceeded 300s timeout")
            raise
        except Exception as e:
            await self._fail_session(type(e).__name__, str(e))
            raise

    def _initial_state(self, app_id):
        return {"application_id": app_id, "session_id": self.session_id,
                "agent_id": self.agent_id, "errors": [],
                "output_events_written": [], "next_agent_triggered": None}

    async def _start_session(self, app_id):
        started_event = {
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {
                "session_id": self.session_id,
                "agent_type": self.agent_type,
                "agent_id": self.agent_id,
                "application_id": app_id,
                "model_version": self.model,
                "langgraph_graph_version": LANGGRAPH_VERSION,
                "context_source": "fresh",
                "context_token_count": 1000,
                "started_at": datetime.now().isoformat(),
            },
        }
        context_loaded_event = {
            "event_type": "AgentContextLoaded",
            "event_version": 1,
            "payload": {
                "session_id": self.session_id,
                "agent_type": self.agent_type,
                "agent_id": self.agent_id,
                "application_id": app_id,
                "model_version": self.model,
                "inputs_validated": [],
                "validation_duration_ms": 0,
                "validated_at": datetime.now().isoformat(),
            },
        }
        await self._append_session([started_event, context_loaded_event])

    async def _record_node_execution(self, name, in_keys, out_keys, ms,
                                      tok_in=None, tok_out=None, cost=None):
        self._seq += 1
        if tok_in is not None:
            self._tokens_input += (tok_in or 0)
            self._tokens_output += (tok_out or 0)
            self._tokens += (tok_in or 0) + (tok_out or 0)
            self._llm_calls += 1
        if cost:
            self._cost += cost
        await self._append_session({"event_type": "AgentNodeExecuted", "event_version": 1, "payload": {
            "session_id": self.session_id, "agent_type": self.agent_type, "node_name": name,
            "node_sequence": self._seq, "input_keys": in_keys, "output_keys": out_keys,
            "llm_called": tok_in is not None, "llm_tokens_input": tok_in,
            "llm_tokens_output": tok_out, "llm_cost_usd": cost,
            "duration_ms": ms, "executed_at": datetime.now().isoformat()}})
        if self._fail_at_node == name:
            raise ValueError(f"Simulated crash at node {name}")

    async def _record_tool_call(self, tool, inp, out, ms):
        await self._append_session({"event_type": "AgentToolCalled", "event_version": 1, "payload": {
            "session_id": self.session_id, "agent_type": self.agent_type, "tool_name": tool,
            "tool_input_summary": inp, "tool_output_summary": out,
            "tool_duration_ms": ms, "called_at": datetime.now().isoformat()}})

    async def _record_output_written(self, events_written, summary):
        await self._append_session({"event_type": "AgentOutputWritten", "event_version": 1, "payload": {
            "session_id": self.session_id, "agent_type": self.agent_type,
            "application_id": self.application_id,
            "events_written": events_written, "output_summary": summary,
            "written_at": datetime.now().isoformat()}})

    async def _complete_session(self, result):
        ms = int((time.time() - self._t0) * 1000)
        await self._append_session({"event_type": "AgentSessionCompleted", "event_version": 1, "payload": {
            "session_id": self.session_id, "agent_type": self.agent_type,
            "application_id": self.application_id,
            "total_nodes_executed": self._seq, "total_llm_calls": self._llm_calls,
            "total_tokens_used": self._tokens,
            "total_tokens_input": self._tokens_input,
            "total_tokens_output": self._tokens_output,
            "total_cost_usd": round(self._cost, 6),
            "total_duration_ms": ms,
            "llm_fallback_count": len(self._llm_errors),
            "llm_errors": self._llm_errors[:5],
            "next_agent_triggered": result.get("next_agent_triggered"),
            "completed_at": datetime.now().isoformat()}})

    async def _fail_session(self, etype, emsg):
        await self._append_session({"event_type": "AgentSessionFailed", "event_version": 1, "payload": {
            "session_id": self.session_id, "agent_type": self.agent_type,
            "application_id": self.application_id,
            "error_type": etype, "error_message": emsg[:500],
            "last_successful_node": f"node_{self._seq}",
            "recoverable": etype in ("llm_timeout", "RateLimitError"),
            "failed_at": datetime.now().isoformat()}})

    async def _append_session(self, event):
        events = event if isinstance(event, list) else [event]
        for attempt in range(MAX_OCC_RETRIES):
            try:
                ver = await self.store.stream_version(self._session_stream)
                await self.store.append(stream_id=self._session_stream,
                                        events=events, expected_version=ver)
                return
            except Exception as e:
                if "OptimisticConcurrencyError" in type(e).__name__ and attempt < MAX_OCC_RETRIES - 1:
                    await asyncio.sleep(0.1 * (2 ** attempt))
                    continue
                raise

    async def _append_stream(self, stream_id: str, event_dict: dict,
                              causation_id: str = None):
        for attempt in range(MAX_OCC_RETRIES):
            try:
                ver = await self.store.stream_version(stream_id)
                await self.store.append(stream_id=stream_id, events=[event_dict],
                                        expected_version=ver, causation_id=causation_id)
                return
            except Exception as e:
                if "OptimisticConcurrencyError" in type(e).__name__ and attempt < MAX_OCC_RETRIES - 1:
                    await asyncio.sleep(0.1 * (2 ** attempt))
                    continue
                raise

    async def _call_llm(self, system, user, max_tokens=1024):
        
        last_exc = None
        for attempt in range(3):
            try:
                resp = await asyncio.wait_for(
                    self.client.chat.completions.create(
                        model=self.model, max_tokens=max_tokens,
                        messages=[{"role": "system", "content": system},
                                  {"role": "user", "content": user}]),
                    timeout=60.0,
                )
                text = resp.choices[0].message.content or ""
                tok_in = resp.usage.prompt_tokens if resp.usage else 0
                tok_out = resp.usage.completion_tokens if resp.usage else 0
                cost = _compute_cost(self.model, tok_in, tok_out)
                return text, tok_in, tok_out, cost
            except (openai.RateLimitError, openai.APIStatusError) as exc:
                last_exc = exc
                if attempt < 2:
                    await asyncio.sleep(2.0 ** attempt * 5)  # 5s, 10s
                continue
            except asyncio.TimeoutError as exc:
                last_exc = exc
                break
        raise last_exc

    @staticmethod
    def _parse_json(content: str):
        if not content:
            return None
        # Strip markdown code fences (e.g. ```json\n...\n```) that some models add
        text = content.strip()
        if text.startswith("```"):
            # Remove opening fence (```json or ```)
            text = text[text.index("\n") + 1:] if "\n" in text else text[3:]
            # Remove closing fence
            if text.rstrip().endswith("```"):
                text = text.rstrip()[:-3].rstrip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Last resort: find the first {...} or [...] block
            import re
            m = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(1))
                except json.JSONDecodeError:
                    pass
            return None

    @staticmethod
    def _sha(d):
        return hashlib.sha256(json.dumps(str(d), sort_keys=True).encode()).hexdigest()[:16]

    async def _is_node_completed(self, node_name: str) -> bool:
        events = await self.store.load_stream(self._session_stream)
        return any(e.get("event_type") == "AgentNodeExecuted" and e.get("payload", {}).get("node_name") == node_name for e in events)
