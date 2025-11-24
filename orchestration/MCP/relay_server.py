import uvicorn
from fastapi import FastAPI, Request
import os, json, datetime

from custody.custodian_ledger import log_event
from telemetry.emit_heartbeat import generate_heartbeat

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/heartbeat")
def heartbeat():
    pulse = generate_heartbeat()
    log_event("HEARTBEAT_EMIT", pulse)
    return pulse

@app.post("/relay")
async def relay(request: Request):
    body = await request.json()
    envelope = body.get("envelope")
    tool = body.get("tool")

    if not envelope or not tool:
        log_event("RELAY_INVALID", {"body": body})
        return {"error": "Invalid envelope"}

    log_event("RELAY_RECEIVED", {"tool": tool, "envelope": envelope})
    return {"status": "received", "tool": tool}

if __name__ == "__main__":
    uvicorn.run("relay_server:app", host="0.0.0.0", port=8000)
