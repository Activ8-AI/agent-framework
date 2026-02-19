import uvicorn
from fastapi import FastAPI, HTTPException, Request

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
    try:
        body = await request.json()
    except Exception:
        log_event("RELAY_INVALID", {"body_type": "invalid_json"})
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not isinstance(body, dict):
        log_event("RELAY_INVALID", {"body_type": type(body).__name__})
        raise HTTPException(status_code=400, detail="Invalid envelope")

    envelope = body.get("envelope")
    tool = body.get("tool")

    if not envelope or not tool:
        log_event(
            "RELAY_INVALID",
            {
                "body_type": "dict",
                "keys": list(body.keys())[:50],
                "has_envelope": bool(envelope),
                "has_tool": bool(tool),
            },
        )
        raise HTTPException(status_code=400, detail="Invalid envelope")

    log_event("RELAY_RECEIVED", {"tool": tool, "envelope": envelope})
    return {"status": "received", "tool": tool}

if __name__ == "__main__":
    uvicorn.run("relay_server:app", host="0.0.0.0", port=8000)
