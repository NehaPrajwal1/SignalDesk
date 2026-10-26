"""Prometheus Alertmanager v4 webhook adapter. Never fetches supplied URLs."""
from datetime import datetime, timezone
import hashlib
import os
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core import SERVICES
from platform_ops import redact


class AlertmanagerAlert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    status: Literal["firing", "resolved"]
    labels: dict[str, str] = Field(max_length=50)
    startsAt: datetime
    fingerprint: str = Field(pattern=r"^[a-fA-F0-9]{16}$")

    @field_validator("startsAt")
    @classmethod
    def aware(cls, value):
        if value.tzinfo is None:
            raise ValueError("startsAt requires a timezone")
        return value.astimezone(timezone.utc)

    @field_validator("labels")
    @classmethod
    def supported_labels(cls, value):
        if value.get("service") not in SERVICES:
            raise ValueError("service label must name a supported SignalDesk service")
        if not 1 <= len(value.get("alertname", "").strip()) <= 240:
            raise ValueError("alertname label is required and limited to 240 characters")
        if value.get("severity", "warning") not in {"critical", "warning", "info", "P1", "P2", "P3"}:
            raise ValueError("Unsupported severity label")
        if any(len(k) > 100 or len(v) > 1000 for k, v in value.items()):
            raise ValueError("Label too long")
        return value


class AlertmanagerBatch(BaseModel):
    model_config = ConfigDict(extra="ignore")
    version: Literal["4"]
    alerts: list[AlertmanagerAlert] = Field(min_length=1, max_length=100)
    truncatedAlerts: int = Field(default=0, ge=0, le=0)


def accept_alert(store, alert):
    source = os.getenv("SIGNALDESK_ALERTMANAGER_SOURCE", "default")
    identity = source + ":" + alert.fingerprint.lower() + ":" + alert.startsAt.isoformat()
    key = "alertmanager-" + hashlib.sha256(identity.encode()).hexdigest()
    with store.transaction() as conn:
        row = conn.execute("SELECT * FROM incidents WHERE request_key=?", (key,)).fetchone()
    if row is None and alert.status == "resolved":
        return {"status": "ignored", "reason": "No matching firing alert"}
    replayed = row is not None
    if row is None:
        labels = alert.labels
        severity = {"critical": "P1", "warning": "P2", "info": "P3"}.get(labels.get("severity", "warning"), labels.get("severity", "P2"))
        data = {"service": labels["service"], "severity": severity, "signal": redact(labels["alertname"].strip())[:240], "impact": "Reported by Prometheus Alertmanager; verify impact", "source": "prometheus-alertmanager"}
        item, replayed = store.create(data, key, actor="alertmanager")
    else:
        item = dict(row)
    if alert.status == "resolved":
        with store.transaction(True) as conn:
            current = store.incident(item["id"], conn)
            if current["status"] not in {"Monitoring", "Mitigated"}:
                item = store._update(conn, item["id"], "Monitoring", "Alertmanager reported resolved; human recovery verification required", "alertmanager")
    return {"status": "accepted", "incident_id": item["id"], "duplicate": replayed, "source_status": alert.status}
