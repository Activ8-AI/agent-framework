from custody.custodian_ledger import log_event

def record_evidence(event):
    log_event("TEAMWORK_EVIDENCE", event)
    return {"logged": True}
