import time
from telemetry.emit_heartbeat import generate_heartbeat
from custody.custodian_ledger import log_event

def autonomy_loop():
    while True:
        pulse = generate_heartbeat()
        log_event("AUTONOMY_LOOP", pulse)
        time.sleep(60)

if __name__ == "__main__":
    autonomy_loop()
