import sqlite3
import datetime
import json

DB_PATH = "custody/custodian_ledger.db"

def connect():
    return sqlite3.connect(DB_PATH)

def write_record(event_type, payload):
    conn = connect()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ledger(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            event_type TEXT,
            payload TEXT
        )
    """)
    conn.commit()

    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload_json = json.dumps(payload, default=str, ensure_ascii=False)
    cursor.execute(
        "INSERT INTO ledger (timestamp, event_type, payload) VALUES (?, ?, ?)",
        (timestamp, event_type, payload_json),
    )
    conn.commit()
    conn.close()
