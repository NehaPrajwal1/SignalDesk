"""SignalDesk HTTP and Slack adapters. Run: uvicorn api:app."""
from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import secrets
import time
from typing import Literal
from urllib.parse import parse_qs
import uuid

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel, ConfigDict, Field
import requests

from core import Conflict, NotFound, SERVICES, Store, demo_rows, investigate

from platform_ops import WorkloadEvent, initialize, ingest_workload, workload_rows, dispatch, save_investigation

Service = Literal["Spark", "Flink", "Airflow", "Trino", "Druid", "Notebook Hub"]
Status = Literal["Investigating", "Monitoring", "Awaiting owner", "Mitigated"]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Alert(Model):
    service: Service
    severity: Literal["P1", "P2", "P3"] = "P2"
    signal: str = Field(min_length=3, max_length=240)
    impact: str = Field(default="Unknown", min_length=1, max_length=160)
    source: str = Field(default="webhook", min_length=1, max_length=120)


class Update(Model):
    status: Status
    note: str = Field(default="", max_length=500)
    expected_version: int = Field(ge=1)


class Batch(Model):
    records: list[dict] = Field(min_length=1, max_length=1000)


class AgentOptions(Model):
    use_llm: bool = False


def create_app(db_path=None, token=None, slack_secret=None, demo=None):
    store = Store(db_path or os.getenv("SIGNALDESK_DB_PATH", "signaldesk.db"))
    token = os.getenv("SIGNALDESK_API_TOKEN", "") if token is None else token
    slack_secret = os.getenv("SLACK_SIGNING_SECRET", "") if slack_secret is None else slack_secret
    demo = os.getenv("SIGNALDESK_DEMO", "1") == "1" if demo is None else demo
    registry = CollectorRegistry()
    request_count = Counter("signaldesk_http_requests_total", "HTTP responses", ["route", "method", "status"], registry=registry)
    duration = Histogram("signaldesk_http_duration_seconds", "HTTP request duration", ["route"], registry=registry)
    open_count = Gauge("signaldesk_open_incidents", "Current open incident count", registry=registry)
    ingestion = Counter("signaldesk_ingestion_rows_total", "Telemetry ingestion outcomes", ["outcome"], registry=registry)

    @asynccontextmanager
    async def lifespan(app):
        store.initialize()
        initialize(store)
        if demo:
            if store.quality()["stored_records"] == 0:
                store.ingest(demo_rows())
            if not store.incidents():
                for service in ("Spark", "Airflow", "Trino"):
                    store.create(Alert(service=service, signal="Synthetic upstream errors", impact="Demo team", source="synthetic fixture").model_dump(), "seed-" + service)
        yield

    app = FastAPI(title="SignalDesk", version="2.0.0", lifespan=lifespan)
    app.state.store = store

    def authorize(request: Request):
        if token and not secrets.compare_digest(request.headers.get("authorization", "").encode(), ("Bearer " + token).encode()):
            raise HTTPException(401, "Valid bearer token required")

    @app.exception_handler(NotFound)
    async def missing(request, error):
        return JSONResponse({"detail": str(error)}, status_code=404)

    @app.exception_handler(Conflict)
    async def conflict(request, error):
        return JSONResponse({"detail": str(error)}, status_code=409)

    @app.middleware("http")
    async def observe(request, call_next):
        start = time.perf_counter()
        request_id = uuid.uuid4().hex
        response = await call_next(request)
        route = request.scope.get("route")
        label = getattr(route, "path", "unmatched")
        request_count.labels(label, request.method, response.status_code).inc()
        duration.labels(label).observe(time.perf_counter() - start)
        response.headers["X-Request-ID"] = request_id
        logging.getLogger("signaldesk").info(json.dumps({"request_id": request_id, "route": label, "status": response.status_code}))
        return response

    @app.get("/api/health")
    def health():
        # A database query makes this a readiness check, not a hard-coded OK.
        count = store.quality()["stored_records"]
        return {"status": "ok", "data_mode": "synthetic fixture" if demo else "ingested telemetry", "records": count}

    @app.get("/api/services", dependencies=[Depends(authorize)])
    def services(service: Service | None = None):
        return store.overview(service)

    @app.get("/api/traffic", dependencies=[Depends(authorize)])
    def traffic(service: Service | None = None):
        return store.traffic(service)

    @app.get("/api/quality", dependencies=[Depends(authorize)])
    def quality():
        return store.quality()

    @app.post("/api/telemetry", dependencies=[Depends(authorize)])
    def telemetry(batch: Batch):
        result = store.ingest(batch.records)
        for outcome in ("accepted", "duplicates", "rejected"):
            ingestion.labels(outcome).inc(result[outcome])
        return result

    @app.get("/api/support", dependencies=[Depends(authorize)])
    def support():
        return store.support()

    @app.get("/api/incidents", dependencies=[Depends(authorize)])
    def incidents(service: Service | None = None, limit: int = Query(200, ge=1, le=1000)):
        return store.incidents(service, limit)

    @app.post("/api/alerts", dependencies=[Depends(authorize)])
    def alert(payload: Alert, request: Request, background_tasks: BackgroundTasks):
        key = request.headers.get("Idempotency-Key")
        if key is not None and (not key.strip() or len(key) > 128):
            raise HTTPException(422, "Idempotency-Key must contain 1-128 characters")
        item, replayed = store.create(payload.model_dump(), key)
        if not replayed:
            background_tasks.add_task(save_investigation, store, item["id"], os.getenv("SIGNALDESK_AUTO_LLM", "0") == "1")
        return JSONResponse(item, status_code=200 if replayed else 201, headers={"Idempotency-Replayed": str(replayed).lower()})

    @app.patch("/api/incidents/{ident}", dependencies=[Depends(authorize)])
    def update(ident: str, payload: Update):
        return store.update(ident, payload.status, payload.note, "api-client", payload.expected_version)

    @app.get("/api/incidents/{ident}/events", dependencies=[Depends(authorize)])
    def events(ident: str):
        return store.events(ident)

    @app.get("/api/triage/{ident}", dependencies=[Depends(authorize)])
    def triage(ident: str):
        return investigate(store, ident)

    @app.post("/api/agent/{ident}", dependencies=[Depends(authorize)])
    def agent(ident: str, options: AgentOptions):
        return save_investigation(store, ident, options.use_llm)

    @app.post("/api/workloads", dependencies=[Depends(authorize)])
    def workloads_ingest(event: WorkloadEvent, background_tasks: BackgroundTasks):
        result = ingest_workload(store, event)
        if event.failed or event.restarts >= 3 or event.memory_percent >= 90 or event.cpu_percent >= 95:
            # Replays also reconcile an incident if a previous request stopped after ingestion.
            payload = Alert(service=event.service, signal=event.kind + ": " + event.pod, impact="Workload needs investigation", source="operational telemetry")
            incident, replayed = store.create(payload.model_dump(), "workload-" + event.event_id)
            result["incident_id"] = incident["id"]
            if not replayed:
                background_tasks.add_task(save_investigation, store, incident["id"], os.getenv("SIGNALDESK_AUTO_LLM", "0") == "1")
        return result

    @app.get("/api/workloads", dependencies=[Depends(authorize)])
    def workloads(service: Service | None = None, limit: int = Query(100, ge=1, le=1000)):
        return workload_rows(store, service, limit)

    @app.get("/api/tools/{name}", dependencies=[Depends(authorize)])
    def tool(name: str, service: Service):
        try:
            return dispatch(store, name, {"service": service})
        except ValueError as error:
            raise HTTPException(422, str(error))

    @app.get("/api/investigations/{ident}", dependencies=[Depends(authorize)])
    def investigation(ident: str):
        store.incident(ident)
        with store.transaction() as conn:
            row = conn.execute("SELECT at,result FROM investigations WHERE incident_id=?", (ident,)).fetchone()
        if row is None:
            return {"status": "pending_or_not_run", "retry": "/api/agent/" + ident}
        return {"status": "completed", "at": row[0], "result": json.loads(row[1])}

    @app.post("/slack/commands")
    async def slack(request: Request):
        if not slack_secret:
            raise HTTPException(503, "Slack integration is not configured")
        body = await request.body()
        if len(body) > 16000:
            raise HTTPException(413, "Slack request too large")
        stamp = request.headers.get("x-slack-request-timestamp", "")
        signature = request.headers.get("x-slack-signature", "")
        try:
            timestamp = int(stamp)
        except ValueError:
            raise HTTPException(401, "Invalid Slack timestamp")
        if abs(time.time() - timestamp) > 300:
            raise HTTPException(401, "Expired Slack request")
        expected = "v0=" + hmac.new(slack_secret.encode(), b"v0:" + stamp.encode() + b":" + body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected.encode(), signature.encode()):
            raise HTTPException(401, "Invalid Slack signature")
        form = parse_qs(body.decode("utf-8", errors="replace"))
        team = os.getenv("SLACK_TEAM_ID")
        if team and form.get("team_id", [""])[0] != team:
            raise HTTPException(403, "Workspace not permitted")
        words = form.get("text", ["status"])[0].split()
        action = words[0].lower() if words else "status"
        if action == "status":
            value = store.support()
            return {"response_type": "ephemeral", "text": f"SignalDesk: {value['open']} open incidents, {value['p1_open']} P1; oldest open {value['oldest_open_minutes']} minutes."}
        if len(words) != 2 or action not in ("triage", "ack"):
            return {"response_type": "ephemeral", "text": "Usage: /signaldesk status | triage INC-ID | ack INC-ID"}
        ident = words[1].upper()
        if action == "ack":
            return store.slack_ack(ident, form.get("user_id", ["slack-user"])[0][:80], signature, timestamp)
        result = investigate(store, ident)
        text = result["summary"] + "\n" + result["runbook"]
        return {"response_type": "ephemeral", "text": text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")}

    @app.get("/metrics")
    def metrics():
        open_count.set(store.support()["open"])
        return Response(generate_latest(registry), headers={"Content-Type": CONTENT_TYPE_LATEST})

    return app


app = create_app()
