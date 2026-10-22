"""Optional Slack Socket Mode worker. Credentials are supplied only at runtime."""
import os
import re

import requests
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from core import SERVICES


def backend(method, path, **kwargs):
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = "Bearer " + os.environ["SIGNALDESK_API_TOKEN"]
    response = requests.request(method, os.environ.get("SIGNALDESK_API_URL", "http://127.0.0.1:8000").rstrip("/") + path, headers=headers, timeout=55, **kwargs)
    response.raise_for_status()
    return response


def allowed(team, channel):
    channels = {value.strip() for value in os.getenv("SLACK_ALLOWED_CHANNELS", "").split(",") if value.strip()}
    return bool(os.getenv("SLACK_TEAM_ID")) and team == os.getenv("SLACK_TEAM_ID") and channel in channels


def card(incident):
    return [{"type": "section", "text": {"type": "plain_text", "text": (incident["id"] + " | " + incident["service"] + " | " + incident["status"] + "\n" + incident["signal"])[:2900]}},
            {"type": "actions", "elements": [{"type": "button", "text": {"type": "plain_text", "text": "Investigate (read only)"}, "action_id": "signaldesk_investigate", "value": incident["id"]}]}]


def command_handler(ack, command, client, respond):
    ack()  # Slack requires acknowledgement before network/model work.
    if not allowed(command.get("team_id"), command.get("channel_id")):
        respond("Workspace or channel is not enabled for SignalDesk.")
        return
    words = command.get("text", "").strip().split(maxsplit=1)
    service = words[1] if len(words) == 2 else ""
    if not words or words[0].lower() != "alert" or service not in SERVICES:
        respond("Usage: /signaldesk alert [Spark|Flink|Airflow|Trino|Druid|Notebook Hub]")
        return
    try:
        # Slack's stable trigger_id makes retry creation idempotent.
        trigger = command.get("trigger_id")
        if not trigger:
            respond("Missing Slack request identity.")
            return
        import hashlib
        key = "slack-" + hashlib.sha256(trigger.encode()).hexdigest()
        response = backend("POST", "/api/alerts", headers={"Idempotency-Key": key}, json={"service": service, "signal": "Support investigation requested via Slack", "impact": "Awaiting customer confirmation", "source": "slack"})
        if response.headers.get("Idempotency-Replayed") == "true":
            respond("This request already created an incident; use its investigation button.")
            return
        incident = response.json()
        message = client.chat_postMessage(channel=command["channel_id"], text="SignalDesk incident " + incident["id"], blocks=card(incident), unfurl_links=False, unfurl_media=False)
        result = backend("POST", "/api/agent/" + incident["id"], json={"use_llm": os.getenv("SIGNALDESK_SLACK_LLM", "0") == "1"}).json()
        post_summary(client, command["channel_id"], message["ts"], result)
    except requests.RequestException:
        respond("Backend unavailable. Check SignalDesk before retrying; an incident may already exist.")


def post_summary(client, channel, thread, result):
    text = "Investigation draft — human review required\n" + result.get("llm_draft", result["summary"]) + "\n" + result["runbook"]
    client.chat_postMessage(channel=channel, thread_ts=thread, text="SignalDesk investigation draft", blocks=[{"type": "section", "text": {"type": "plain_text", "text": text[:2900]}}], unfurl_links=False, unfurl_media=False)


def action_handler(ack, body, client, respond):
    ack()
    channel = body.get("channel", {}).get("id")
    if not allowed(body.get("team", {}).get("id"), channel):
        respond("Workspace or channel is not enabled for SignalDesk.")
        return
    actions = body.get("actions") or []
    ident = actions[0].get("value", "") if actions else ""
    if not re.fullmatch(r"INC-[A-F0-9]+", ident):
        respond("Invalid incident identity.")
        return
    try:
        result = backend("POST", "/api/agent/" + ident, json={"use_llm": os.getenv("SIGNALDESK_SLACK_LLM", "0") == "1"}).json()
        message = body.get("message", {})
        post_summary(client, channel, message.get("thread_ts") or message["ts"], result)
    except (requests.RequestException, KeyError):
        respond("Investigation unavailable. Retry from SignalDesk.")


def build_app():
    for name in ("SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "SLACK_TEAM_ID", "SLACK_ALLOWED_CHANNELS", "SIGNALDESK_API_TOKEN"):
        if not os.getenv(name):
            raise RuntimeError(name + " is required")
    app = App(token=os.environ["SLACK_BOT_TOKEN"])
    app.command("/signaldesk")(command_handler)
    app.action("signaldesk_investigate")(action_handler)
    return app


if __name__ == "__main__":
    SocketModeHandler(build_app(), os.environ["SLACK_APP_TOKEN"]).start()
