import sqlite3
import os

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

    cursor.execute(
        "INSERT INTO ledger (timestamp, event_type, payload) VALUES (?, ?, ?)",
        (os.popen("date").read().strip(), event_type, str(payload)),
    )
    conn.commit()
    conn.close()
