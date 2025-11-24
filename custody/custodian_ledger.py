from memory.sql_store.sql_store import write_record

def log_event(event_type, payload):
    write_record(event_type, payload)
