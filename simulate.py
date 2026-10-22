"""Small synthetic workload stream. Never contacts real clusters."""
import argparse
from datetime import datetime, timezone
import os
import time
import uuid

import requests


def sample(index, run_id):
    services = ("Airflow", "Spark", "Trino", "Notebook Hub")
    service = services[index % 4]
    failed = index % 7 == 0
    kind = {"Airflow": "dag_failure", "Spark": "job_memory", "Trino": "query_error", "Notebook Hub": "container_health"}[service]
    return {"event_id": f"sim-{run_id}-{index}", "at": datetime.now(timezone.utc).isoformat(), "service": service,
            "kind": kind, "pod": service.lower().replace(" ", "-") + "-demo-0", "cpu_percent": 97 if failed else 30,
            "memory_percent": 94 if failed else 45, "latency_ms": 1800 if failed else 120,
            "restarts": 4 if failed else 0, "failed": failed,
            "log": "SYNTHETIC: " + (kind + " observed; investigate upstream dependencies" if failed else "workload completed")}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=60)
    parser.add_argument("--interval", type=float, default=1)
    args = parser.parse_args()
    if not 1 <= args.count <= 10000 or not 0.1 <= args.interval <= 60:
        parser.error("count must be 1..10000; interval 0.1..60 seconds")
    base = os.getenv("SIGNALDESK_API_URL", "http://127.0.0.1:8000").rstrip("/")
    headers = {"Authorization": "Bearer " + os.getenv("SIGNALDESK_API_TOKEN", "")}
    run_id = uuid.uuid4().hex[:12]
    for index in range(args.count):
        event = sample(index, run_id)
        response = requests.post(base + "/api/workloads", json=event, headers=headers, timeout=15)
        response.raise_for_status()
        row = {"event_id": event["event_id"], "at": event["at"], "service": event["service"], "status_code": 503 if event["failed"] else 200, "latency_ms": event["latency_ms"], "message": event["log"]}
        response = requests.post(base + "/api/telemetry", json={"records": [row]}, headers=headers, timeout=15)
        response.raise_for_status()
        print(event["event_id"], event["service"], "failed" if event["failed"] else "healthy", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
