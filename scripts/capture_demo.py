"""Run both services, verify browser rendering, and capture a CI demo screenshot."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

import requests
from playwright.sync_api import expect, sync_playwright


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_ready(url, process):
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError("Service exited before becoming ready")
        try:
            if requests.get(url, timeout=1).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(.5)
    raise RuntimeError("Service readiness deadline exceeded")


def main():
    output = Path("verification")
    output.mkdir(exist_ok=True)
    api_port, ui_port = free_port(), free_port()
    processes = []
    with tempfile.TemporaryDirectory() as data, (output / "api.log").open("w") as api_log, (output / "dashboard.log").open("w") as dashboard_log:
        env = {**os.environ, "SIGNALDESK_DB_PATH": str(Path(data) / "demo.db"), "SIGNALDESK_API_TOKEN": "browser-test-token", "SIGNALDESK_DEMO": "1", "SIGNALDESK_API_URL": f"http://127.0.0.1:{api_port}"}
        try:
            api = subprocess.Popen([sys.executable, "-m", "uvicorn", "api:app", "--host", "127.0.0.1", "--port", str(api_port)], env=env, stdout=api_log, stderr=subprocess.STDOUT)
            processes.append(api)
            wait_ready(env["SIGNALDESK_API_URL"] + "/api/health", api)
            ui = subprocess.Popen([sys.executable, "-m", "streamlit", "run", "app.py", "--server.headless=true", "--server.address=127.0.0.1", f"--server.port={ui_port}", "--browser.gatherUsageStats=false"], env=env, stdout=dashboard_log, stderr=subprocess.STDOUT)
            processes.append(ui)
            wait_ready(f"http://127.0.0.1:{ui_port}/_stcore/health", ui)
            with sync_playwright() as browser_engine:
                browser = browser_engine.chromium.launch()
                page = browser.new_page(viewport={"width": 1440, "height": 1100})
                page.goto(f"http://127.0.0.1:{ui_port}")
                page.get_by_role("heading", name="Incident queue", exact=True).wait_for(timeout=60000)
                try:
                    expect(page.get_by_text("Request success (sample)", exact=True)).to_be_visible(timeout=60000)
                    expect(page.get_by_text("Service health and ownership", exact=True)).to_be_visible(timeout=60000)
                    expect(page.locator('[data-testid="stException"]')).to_have_count(0)
                finally:
                    (output / "browser-text.txt").write_text(page.locator("body").inner_text())
                    page.screenshot(path=str(output / "dashboard.png"), full_page=True)
                    browser.close()
            (output / "browser-result.json").write_text(json.dumps({"result": "passed", "checks": ["API readiness", "dashboard HTTP readiness", "rendered telemetry", "rendered incident queue", "no Streamlit exception"], "data": "deterministic synthetic fixture"}, indent=2))
        finally:
            for process in reversed(processes):
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
