from pathlib import Path
"""Execute the actual Streamlit script against an actual HTTP API server."""
import socket
import threading
import time
import pytest
import requests
import uvicorn
from streamlit.testing.v1 import AppTest
from api import create_app


@pytest.fixture
def live_api(tmp_path, monkeypatch):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    app = create_app(tmp_path / "ui.db", token="ui-token", slack_secret="", demo=True)
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(.05)
    assert server.started, "API failed to start"
    monkeypatch.setenv("SIGNALDESK_API_URL", url)
    monkeypatch.setenv("SIGNALDESK_API_TOKEN", "ui-token")
    try:
        yield url
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


def widget(collection, label):
    return next(item for item in collection if item.label == label)


def test_dashboard_loads_and_filters_with_live_api(live_api):
    page = AppTest.from_file(Path(__file__).resolve().parents[1] / "app.py", default_timeout=20).run()
    assert not page.exception
    assert len(page.metric) == 7
    assert widget(page.metric, "Telemetry records in scope").value == "360"
    widget(page.selectbox, "Service scope").set_value("Spark").run()
    assert not page.exception
    assert widget(page.metric, "Telemetry records in scope").value == "60"
    widget(page.multiselect, "Severity").set_value([]).run()
    assert not page.exception
    assert any("No incidents match" in item.value for item in page.info)


def test_dashboard_creates_and_updates_incident(live_api):
    headers = {"Authorization": "Bearer ui-token"}
    before = requests.get(live_api + "/api/incidents", headers=headers, timeout=5).json()
    page = AppTest.from_file(Path(__file__).resolve().parents[1] / "app.py", default_timeout=20).run()
    widget(page.text_input, "Observed signal").set_value("Dashboard integration scenario")
    widget(page.button, "Create incident").click().run()
    assert not page.exception
    after = requests.get(live_api + "/api/incidents", headers=headers, timeout=5).json()
    assert len(after) == len(before) + 1
    created = next(item for item in after if item["signal"] == "Dashboard integration scenario")
    widget(page.selectbox, "Incident").set_value(created["id"]).run()
    widget(page.selectbox, "New status").set_value("Mitigated")
    widget(page.button, "Save update").click().run()
    assert not page.exception
    events = requests.get(live_api + f"/api/incidents/{created['id']}/events", headers=headers, timeout=5).json()
    assert events[0]["new_status"] == "Mitigated"


def test_dashboard_handles_bad_token(live_api, monkeypatch):
    monkeypatch.setenv("SIGNALDESK_API_TOKEN", "wrong")
    page = AppTest.from_file(Path(__file__).resolve().parents[1] / "app.py", default_timeout=20).run()
    assert not page.exception
    assert any("401" in item.value for item in page.error)


def test_dashboard_handles_api_outage(monkeypatch):
    monkeypatch.setenv("SIGNALDESK_API_URL", "http://127.0.0.1:1")
    page = AppTest.from_file(Path(__file__).resolve().parents[1] / "app.py", default_timeout=20).run()
    assert not page.exception
    assert any("unreachable" in item.value for item in page.error)
