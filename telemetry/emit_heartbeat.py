import datetime
from custody.custodian_ledger import log_event

def generate_heartbeat():
    pulse = {
        "timestamp": datetime.datetime.now().isoformat(),
        "status": "alive",
        "drift": 0,
        "load": "nominal",
        "agent_state": "active"
    }
    return pulse
