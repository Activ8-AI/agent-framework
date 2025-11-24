import os, json

VECTOR_DB = "memory/vector_store/vector_index.json"

def load_vectors():
    if not os.path.exists(VECTOR_DB):
        return {}
    with open(VECTOR_DB, "r") as f:
        return json.load(f)

def save_vectors(data):
    os.makedirs("memory/vector_store", exist_ok=True)
    with open(VECTOR_DB, "w") as f:
        json.dump(data, f, indent=2)
