"""Executable evidence for workload ingestion, tools, MCP and ChatOps boundaries."""
import asyncio
import json
import os
import socket
import sys
import threading
import time
from unittest.mock import Mock

from fastapi.testclient import TestClient
import pytest
import requests
import uvicorn

from api import create_app
from core import Store
from platform_ops import WorkloadEvent, dispatch, initialize, ingest_workload, investigate_agent, redact
from simulate import sample
import slack_bot


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path / "platform.db", token="test", demo=False)) as value:
        value.headers["Authorization"] = "Bearer test"
        yield value


def test_operational_alert_replay_and_persisted_investigation(client):
    event = sample(0, "golden")
    first = client.post("/api/workloads", json=event)
    assert first.status_code == 200 and first.json()["accepted"]
    ident = first.json()["incident_id"]
    replay = client.post("/api/workloads", json=event).json()
    assert replay["duplicate"] and replay["incident_id"] == ident
    assert len(client.get("/api/incidents").json()) == 1
    saved = client.get("/api/investigations/" + ident).json()
    assert saved["status"] == "completed"
    assert len(saved["result"]["agent_trace"]) == 3
    assert len(client.get("/api/workloads").json()) == 1
    changed = {**event, "cpu_percent": 10}
    assert client.post("/api/workloads", json=changed).status_code == 409


@pytest.mark.parametrize("field,value", [("cpu_percent", 101), ("memory_percent", -1), ("at", "2026-01-01T00:00:00"), ("service", "unknown"), ("pod", "../secret"), ("restarts", True), ("extra", "bad"), ("latency_ms", -1)])
def test_bad_workload_rejected(client, field, value):
    event = sample(1, "bad")
    event[field] = value
    assert client.post("/api/workloads", json=event).status_code == 422
    assert client.get("/api/workloads").json() == []


def test_healthy_sample_no_alert_and_redacted_logs(client):
    event = sample(1, "healthy")
    event["log"] = "password=secret token:abc Bearer xyz"
    assert client.post("/api/workloads", json=event).status_code == 200
    assert client.get("/api/incidents").json() == []
    logs = client.get("/api/tools/fetch_pod_logs?service=Spark").json()
    assert "secret" not in json.dumps(logs) and "xyz" not in json.dumps(logs)
    assert "REDACTED" in json.dumps(logs)
    assert client.get("/api/tools/shell?service=Spark").status_code == 422
    assert client.get("/api/tools/check_service_health?service=secret").status_code == 422
    assert client.get("/api/workloads?limit=1001").status_code == 422
    assert client.get("/api/workloads", headers={"Authorization": "wrong"}).status_code == 401


@pytest.fixture
def investigation_store(tmp_path):
    store = Store(tmp_path / "agent.db")
    store.initialize()
    initialize(store)
    item, _ = store.create({"service": "Spark", "severity": "P2", "signal": "memory pressure", "impact": "delayed jobs", "source": "test"}, None)
    ingest_workload(store, WorkloadEvent(**sample(1, "agent")))
    return store, item["id"]


def response(message):
    value = Mock()
    value.json.return_value = {"message": message}
    return value


def tool_call(name="fetch_pod_logs", service="Spark"):
    return {"role": "assistant", "tool_calls": [{"function": {"name": name, "arguments": {"service": service}}}]}


def test_dynamic_agent_consumes_tool_results(investigation_store):
    store, ident = investigation_store
    post = Mock(side_effect=[response(tool_call()), response(tool_call("search_runbooks")), response({"role": "assistant", "content": "Hypothesis only; consult RB-SPARK."})])
    result = investigate_agent(store, ident, True, post)
    assert result["llm_status"] == "generated_unverified"
    assert [row["tool"] for row in result["agent_trace"]] == ["fetch_pod_logs", "search_runbooks"]
    messages = post.call_args.kwargs["json"]["messages"]
    assert any(row["role"] == "tool" and "sim-agent-1" in row["content"] for row in messages)
    assert store.incident(ident)["status"] == "Investigating"


@pytest.mark.parametrize("name,service", [("shell", "Spark"), ("check_service_health", "Trino"), ("fetch_pod_logs", "../../etc")])
def test_agent_cannot_expand_scope(investigation_store, name, service):
    store, ident = investigation_store
    result = investigate_agent(store, ident, True, Mock(return_value=response(tool_call(name, service))))
    assert result["llm_status"] == "unavailable_using_evidence_summary"
    assert result["agent_trace"] == []


def test_agent_budget_and_malformed_output(investigation_store):
    store, ident = investigation_store
    post = Mock(return_value=response(tool_call()))
    result = investigate_agent(store, ident, True, post)
    assert post.call_count == 4
    assert result["llm_status"] == "budget_exhausted_using_evidence_summary"
    assert investigate_agent(store, ident, True, Mock(return_value=response({"role": "assistant", "tool_calls": [{}] * 7})))["llm_status"].startswith("unavailable")
    for args in ({"service": "Spark", "command": "ls"}, [], {"service": "Trino"}):
        with pytest.raises(ValueError):
            dispatch(store, "fetch_pod_logs", args, "Spark")
    assert redact("token=abc") == "token=[REDACTED]"


@pytest.fixture
def live_api(tmp_path):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(create_app(tmp_path / "mcp.db", token="test", demo=True), log_level="error"))
    thread = threading.Thread(target=lambda: server.run(sockets=[sock]), daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            if requests.get(url + "/api/health", timeout=0.2).status_code == 200:
                break
        except requests.RequestException:
            time.sleep(0.05)
    else:
        pytest.fail("Backend did not become ready")
    yield url
    server.should_exit = True
    thread.join(timeout=5)
    sock.close()


def test_real_mcp_stdio_handshake_and_tools(live_api):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    async def scenario():
        params = StdioServerParameters(command=sys.executable, args=["mcp_server.py"], env={**os.environ, "SIGNALDESK_API_URL": live_api, "SIGNALDESK_API_TOKEN": "test"})
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                listed = await session.list_tools()
                assert {tool.name for tool in listed.tools} == {"fetch_pod_logs", "search_runbooks", "check_service_health"}
                for name in [tool.name for tool in listed.tools]:
                    result = await session.call_tool(name, {"service": "Spark"})
                    assert not result.isError and result.content
                invalid = await session.call_tool("fetch_pod_logs", {"service": "unknown"})
                assert invalid.isError
                unknown = await session.call_tool("execute_shell", {"command": "ls"})
                assert unknown.isError
    asyncio.run(asyncio.wait_for(scenario(), timeout=30))


def test_mcp_http_gate_fails_closed(monkeypatch):
    from mcp_server import app
    monkeypatch.delenv("SIGNALDESK_MCP_TOKEN", raising=False)
    with TestClient(app) as client:
        assert client.post("/mcp", json={}).status_code == 401
        monkeypatch.setenv("SIGNALDESK_MCP_TOKEN", "private")
        assert client.post("/mcp", headers={"Authorization": "Bearer wrong"}, json={}).status_code == 401
        assert client.post("/mcp", headers={"Authorization": "Bearer private", "Host": "localhost", "Accept": "application/json, text/event-stream"}, json={}).status_code != 401


def test_slack_command_ack_order_thread_and_retry(monkeypatch):
    monkeypatch.setenv("SLACK_TEAM_ID", "T1")
    monkeypatch.setenv("SLACK_ALLOWED_CHANNELS", "C1")
    calls = []
    ack = lambda: calls.append("ack")
    respond = Mock()
    client = Mock()
    client.chat_postMessage.return_value = {"ts": "123.4"}
    incident = {"id": "INC-AB12", "service": "Spark", "status": "Investigating", "signal": "test"}
    created = Mock(headers={})
    created.json.return_value = incident
    triaged = Mock()
    triaged.json.return_value = {"summary": "Evidence summary", "runbook": "Inspect memory"}
    def backend(*args, **kwargs):
        assert calls == ["ack"]
        return created if args[1] == "/api/alerts" else triaged
    monkeypatch.setattr(slack_bot, "backend", backend)
    command = {"team_id": "T1", "channel_id": "C1", "trigger_id": "abc", "text": "alert Spark"}
    slack_bot.command_handler(ack, command, client, respond)
    assert client.chat_postMessage.call_count == 2
    assert client.chat_postMessage.call_args.kwargs["thread_ts"] == "123.4"
    assert client.chat_postMessage.call_args.kwargs["blocks"][0]["text"]["type"] == "plain_text"
    calls.clear()
    created.headers = {"Idempotency-Replayed": "true"}
    client.reset_mock()
    slack_bot.command_handler(ack, command, client, respond)
    client.chat_postMessage.assert_not_called()


def test_slack_denied_and_button_thread(monkeypatch):
    monkeypatch.setenv("SLACK_TEAM_ID", "T1")
    monkeypatch.setenv("SLACK_ALLOWED_CHANNELS", "C1")
    client, respond, ack = Mock(), Mock(), Mock()
    slack_bot.command_handler(ack, {"team_id": "other", "channel_id": "C1"}, client, respond)
    ack.assert_called_once()
    client.chat_postMessage.assert_not_called()
    result = Mock()
    result.json.return_value = {"summary": "Check", "runbook": "Runbook"}
    monkeypatch.setattr(slack_bot, "backend", Mock(return_value=result))
    body = {"team": {"id": "T1"}, "channel": {"id": "C1"}, "actions": [{"value": "INC-AB12"}], "message": {"ts": "4", "thread_ts": "3"}}
    slack_bot.action_handler(ack, body, client, respond)
    assert client.chat_postMessage.call_args.kwargs["thread_ts"] == "3"
    body["actions"][0]["value"] = "../../secret"
    client.reset_mock()
    slack_bot.action_handler(ack, body, client, respond)
    client.chat_postMessage.assert_not_called()
