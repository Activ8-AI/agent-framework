from custody.custodian_ledger import log_event

def emit_signal(message):
    log_event("SLACK_SIGNAL", {"message": message})
    return {"sent": True}
