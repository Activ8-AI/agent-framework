import os
import requests
from custody.custodian_ledger import log_event

def load_secrets():
    token = os.environ.get("NOTION_SECRETS_TOKEN")
    if not token:
        raise Exception("NOTION_SECRETS_TOKEN missing from env")

    # Simulated placeholder fetch
    secrets = {
        "SLACK_BOT_TOKEN": "placeholder",
        "TEAMWORK_API_KEY": "placeholder",
        "HUBSPOT_KEY": "placeholder",
    }

    for k, v in secrets.items():
        os.environ[k] = v

    log_event("SECRETS_LOADED", list(secrets.keys()))
    return secrets

if __name__ == "__main__":
    load_secrets()
