from custody.custodian_ledger import log_event

def send_to_notion(payload):
    log_event("NOTION_RELAY", payload)
    return {"status": "ok"}
