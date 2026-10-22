from __future__ import annotations
import hashlib, hmac, os, secrets, sqlite3, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

BASE = Path(__file__).resolve().parent
DB = Path(os.getenv("SIGNALDESK_DB_PATH", str(BASE / "signaldesk.db")))
TOKEN = os.getenv("SIGNALDESK_API_TOKEN", "")
SLACK_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
SERVICES = ["Spark", "Flink", "Airflow", "Trino", "Druid", "Notebook Hub"]
RUNBOOKS = {"Spark":"Check executor saturation, failed stages, shuffle spill, and recent job changes.","Flink":"Check checkpoint duration, backpressure, task restarts, and source lag.","Airflow":"Check scheduler heartbeat, queued task age, worker capacity, and recent DAG edits.","Trino":"Check coordinator health, queued queries, worker availability, and connector errors.","Druid":"Check ingestion task failures, segment availability, and broker query latency.","Notebook Hub":"Check pod scheduling, image pull errors, and per-user resource quotas."}
ALERTS = Counter("signaldesk_alerts_accepted_total", "Alerts accepted by SignalDesk")
UPDATES = Counter("signaldesk_incident_updates_total", "Incident status updates")
OPEN = Gauge("signaldesk_open_incidents", "Incidents not marked Mitigated")
app = FastAPI(title="SignalDesk API", version="1.0.0", description="Incident intake and support workflow demo for a data platform.")

def now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")
def db():
    c=sqlite3.connect(DB, timeout=10); c.row_factory=sqlite3.Row; return c
def init_db():
    DB.parent.mkdir(parents=True, exist_ok=True)
    with db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS incidents(id TEXT PRIMARY KEY,opened_at TEXT,severity TEXT,service TEXT,signal TEXT,status TEXT,impact TEXT,source TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,incident_id TEXT,at TEXT,actor TEXT,old_status TEXT,new_status TEXT,note TEXT)")
        if c.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]==0:
            seeds=[("INC-2401","P1","Spark","Executor failures above threshold","Investigating","2 teams"),("INC-2402","P2","Airflow","Scheduler queue age elevated","Monitoring","4 pipelines"),("INC-2403","P2","Trino","Query latency regression","Awaiting owner","1 team"),("INC-2404","P3","Flink","Checkpoint duration increased","Investigating","1 pipeline")]
            for ident,sev,svc,sig,status,impact in seeds:
                c.execute("INSERT INTO incidents VALUES(?,?,?,?,?,?,?,?)",(ident,now(),sev,svc,sig,status,impact,"synthetic seed"))
                c.execute("INSERT INTO events(incident_id,at,actor,old_status,new_status,note) VALUES(?,?,?,?,?,?)",(ident,now(),"system",None,status,"Synthetic demo incident created"))
@app.on_event("startup")
def startup(): init_db()
class Alert(BaseModel):
    service:str; severity:str=Field(default="P2", pattern="^P[123]$"); signal:str=Field(min_length=3,max_length=240); impact:str=Field(default="Unknown",max_length=160); source:str=Field(default="webhook",max_length=120)
class StatusUpdate(BaseModel):
    status:str=Field(pattern="^(Investigating|Monitoring|Awaiting owner|Mitigated)$"); note:str=Field(default="",max_length=500); actor:str=Field(default="dashboard",max_length=80)
def asdict(r): return dict(r)
def auth(req):
    if TOKEN and not secrets.compare_digest(req.headers.get("authorization",""),"Bearer "+TOKEN): raise HTTPException(401,"A valid bearer token is required")
def get_incident(i):
    with db() as c: r=c.execute("SELECT * FROM incidents WHERE id=?",(i,)).fetchone()
    if not r: raise HTTPException(404,"Incident not found")
    return asdict(r)
def gauge():
    with db() as c: n=c.execute("SELECT COUNT(*) FROM incidents WHERE status!='Mitigated'").fetchone()[0]
    OPEN.set(n)
def create(a):
    if a.service not in SERVICES: raise HTTPException(422,"Unknown service")
    with db() as c:
        n=c.execute("SELECT MAX(CAST(SUBSTR(id,5) AS INTEGER)) FROM incidents").fetchone()[0] or 2400; i=f"INC-{n+1}"; t=now()
        c.execute("INSERT INTO incidents VALUES(?,?,?,?,?,?,?,?)",(i,t,a.severity,a.service,a.signal.strip(),"Investigating",a.impact.strip(),a.source.strip()))
        c.execute("INSERT INTO events(incident_id,at,actor,old_status,new_status,note) VALUES(?,?,?,?,?,?)",(i,t,"alert-ingestion",None,"Investigating",f"Created from {a.source}"))
    ALERTS.inc(); gauge(); return get_incident(i)
def set_status(i,u):
    old=get_incident(i)
    with db() as c:
        c.execute("UPDATE incidents SET status=? WHERE id=?",(u.status,i))
        c.execute("INSERT INTO events(incident_id,at,actor,old_status,new_status,note) VALUES(?,?,?,?,?,?)",(i,now(),u.actor.strip() or "dashboard",old["status"],u.status,u.note.strip()))
    UPDATES.inc(); gauge(); return get_incident(i)
def triage_data(i):
    x=get_incident(i); rb=RUNBOOKS.get(x["service"],"Confirm the signal, inspect recent changes, and contact the service owner.")
    return {"incident_id":i,"runbook":rb,"customer_update":f"We are investigating {x['signal']} affecting {x['impact']}. The service team is checking relevant health signals; we will share an update when the scope is confirmed."}
@app.get("/api/health")
def health(): return {"status":"ok","data_mode":"sqlite + synthetic telemetry","database":DB.name}
@app.get("/api/services")
def services():
    owners=["Compute","Streaming","Orchestration","Query","Analytics","Developer Experience"]
    return [{"service":s,"availability_pct":round(99.65+((i*7)%30)/100,2),"error_rate_pct":round(.15+((i*11)%18)/100,2),"p95_latency_ms":180+i*73,"owner":owners[i]} for i,s in enumerate(SERVICES)]
@app.get("/api/traffic")
def traffic():
    base=datetime.now(timezone.utc).replace(minute=0,second=0,microsecond=0).timestamp()
    return [{"time":base-(23-h)*3600,"requests_k":round(18+4*((h*17)%13)/12+(h%5),2),"error_rate_pct":round(.1+((h*7)%24)/100,2)} for h in range(24)]
@app.get("/api/incidents")
def incidents():
    with db() as c: rows=c.execute("SELECT * FROM incidents ORDER BY opened_at DESC").fetchall()
    return [asdict(r) for r in rows]
@app.get("/api/incidents/{i}/events")
def events(i):
    get_incident(i)
    with db() as c: rows=c.execute("SELECT at,actor,old_status,new_status,note FROM events WHERE incident_id=? ORDER BY id DESC",(i,)).fetchall()
    return [asdict(r) for r in rows]
@app.get("/api/triage/{i}")
def triage(i): return triage_data(i)
@app.post("/api/alerts",status_code=201)
def ingest(a:Alert,request:Request): auth(request); return create(a)
@app.patch("/api/incidents/{i}")
def update(i:str,u:StatusUpdate,request:Request): auth(request); return set_status(i,u)
@app.post("/slack/commands")
async def slack(request:Request):
    if not SLACK_SECRET: raise HTTPException(503,"Set SLACK_SIGNING_SECRET to enable Slack verification")
    body=await request.body(); stamp=request.headers.get("X-Slack-Request-Timestamp",""); sig=request.headers.get("X-Slack-Signature","")
    try: ts=int(stamp)
    except ValueError: raise HTTPException(401,"Invalid Slack timestamp")
    if abs(time.time()-ts)>300: raise HTTPException(401,"Slack request timestamp expired")
    digest=hmac.new(SLACK_SECRET.encode(),b"v0:"+stamp.encode()+b":"+body,hashlib.sha256).hexdigest()
    if not hmac.compare_digest("v0="+digest,sig): raise HTTPException(401,"Invalid Slack signature")
    form=parse_qs(body.decode("utf-8",errors="replace")); words=form.get("text",[""])[0].strip().split(); user=form.get("user_name",["slack-user"])[0][:80]
    if not words or words[0].lower()=="status":
        items=[x for x in incidents() if x["status"]!="Mitigated"]
        lines=[f"{x['severity']} {x['id']} · {x['service']} · {x['status']} · {x['signal']}" for x in items[:10]]
        return {"response_type":"ephemeral","text":"Open SignalDesk incidents:\n"+("\n".join(lines) or "No open incidents.")}
    action=words[0].lower()
    if len(words)<2: return {"response_type":"ephemeral","text":"Usage: /signaldesk status | triage INC-2401 | ack INC-2401"}
    ident=words[1].upper()
    try: item=get_incident(ident)
    except HTTPException: return {"response_type":"ephemeral","text":f"Unknown incident: {ident}"}
    if action=="triage": return {"response_type":"ephemeral","text":f"{item['severity']} {ident} · {item['service']}\n{item['signal']}\nFirst checks: {triage_data(ident)['runbook']}"}
    if action=="ack": set_status(ident,StatusUpdate(status="Investigating",note=f"Acknowledged in Slack by {user}",actor=user)); return {"response_type":"in_channel","text":f"{user} acknowledged {ident}; status set to Investigating."}
    return {"response_type":"ephemeral","text":"Usage: /signaldesk status | triage INC-2401 | ack INC-2401"}
@app.get("/metrics")
def metrics(): gauge(); return PlainTextResponse(generate_latest(),media_type=CONTENT_TYPE_LATEST)
