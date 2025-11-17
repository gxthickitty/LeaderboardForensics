import os
import json
import time
import sys
import signal
from typing import Dict, Any
import requests
from requests.adapters import HTTPAdapter, Retry

HOSTNAMES = {
    "www": "https://www.kogama.com/",
    "br": "https://www.kogama.com.br/",
    "friends": "https://friends.kogama.com/"
}
ENDPOINT = "api/leaderboard/top/"
COUNT = 400
REQUEST_TIMEOUT = 10.0
SAVE_INTERVAL = 30

session = requests.Session()
retries = Retry(total=5, backoff_factor=0.8, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))

_stop = False
_last_save = 0

def atomic_write(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
        f.flush()
    os.replace(tmp, path)

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def load_json_if_exists(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def build_url(base: str, page: int) -> str:
    return f"{base.rstrip('/')}/{ENDPOINT}?count={COUNT}&page={page}"

def normalize_id(entry: Dict[str, Any]) -> str:
    for k in ("id","profile_id","user_id","player_id","profileId","playerId","id_str"):
        if k in entry and entry[k] is not None:
            return str(entry[k])
    return json.dumps(entry, sort_keys=True)

def merge_into_store(store: Dict[str, Any], entries: list, page: int) -> None:
    for ent in entries:
        uid = normalize_id(ent)
        bucket = store.setdefault(uid, {"latest": None, "history": [], "pages": []})
        bucket["history"].append(ent)
        bucket["latest"] = ent
        if page not in bucket["pages"]:
            bucket["pages"].append(page)

def fetch_page(url: str):
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        return {}

def graceful(sig, frame):
    global _stop
    _stop = True

signal.signal(signal.SIGINT, graceful)
signal.signal(signal.SIGTERM, graceful)

def run(server_key: str):
    global _last_save, _stop
    base = HOSTNAMES[server_key]
    outdir = os.path.join("data", server_key)
    ensure_dir(outdir)
    last_path = os.path.join(outdir, "last.json")
    users_path = os.path.join(outdir, "users.json")
    last = load_json_if_exists(last_path, {"page": 1})
    store = load_json_if_exists(users_path, {})

    start_page = last.get("page", 1)
    if start_page <= 0:
        start_page = 1

    page = start_page
    total_expected = None

    while True:
        if _stop:
            last["page"] = page
            atomic_write(last_path, last)
            atomic_write(users_path, store)
            break
        url = build_url(base, page)
        try:
            resp = fetch_page(url)
        except Exception:
            time.sleep(3)
            continue

        data_array = []
        if isinstance(resp, dict) and "data" in resp:
            data_array = resp.get("data") or []
            total_expected = resp.get("total", total_expected)
        elif isinstance(resp, list):
            data_array = resp
        else:
            data_array = []

        if not data_array:
            last["page"] = page
            last["complete"] = True
            atomic_write(last_path, last)
            atomic_write(users_path, store)
            break

        merge_into_store(store, data_array, page)
        last["page"] = page + 1
        atomic_write(users_path, store)
        atomic_write(last_path, last)

        if total_expected:
            collected = sum(len(v.get("history", [])) for v in store.values())
            try:
                pct = (collected * 100.0) / int(total_expected)
            except Exception:
                pct = 0.0
            print(f"{pct:.6f}% page={page} collected={collected} total={total_expected}")
        else:
            collected = sum(len(v.get("history", [])) for v in store.values())
            print(f"page={page} collected={collected}")

        page += 1
        now = time.time()
        if now - _last_save > SAVE_INTERVAL:
            atomic_write(users_path, store)
            atomic_write(last_path, last)
            _last_save = now
        time.sleep(1)

if __name__ == "__main__":
    choice = input("Enter server [br,www,friends]: ").strip().lower()
    if choice not in HOSTNAMES:
        print("Invalid server.")
        sys.exit(1)
    try:
        run(choice)
    except Exception as e:
        print("Error:", e)
        sys.exit(1)
    print("Finished.")
