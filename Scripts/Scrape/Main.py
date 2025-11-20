import os
import json
import time
import sys
import signal
from typing import Dict, Any, Tuple
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
BUCKET_SIZE = 5000

session = requests.Session()
retries = Retry(total=5, backoff_factor=0.8, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))

_stop = False
_last_save = 0

def atomic_write(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write("\n")
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass
    os.replace(tmp, path)

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

def rank_bucket_bounds(rank: int) -> Tuple[int,int]:
    if rank <= 0:
        return (0,0)
    start = ((rank - 1) // BUCKET_SIZE) * BUCKET_SIZE + 1
    end = start + BUCKET_SIZE - 1
    return (start, end)

def bucket_path(root: str, start: int, end: int) -> str:
    folder = os.path.join(root, f"{start}to{end}")
    return os.path.join(folder, "data.json")

def fetch_page(url: str):
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        return {}

class BucketManager:
    def __init__(self, root: str):
        self.root = root
        self._cache: Dict[Tuple[int,int], Dict[str, Any]] = {}
        self._dirty: Dict[Tuple[int,int], bool] = {}

    def _load_bucket(self, start: int, end: int) -> Dict[str, Any]:
        key = (start, end)
        if key in self._cache:
            return self._cache[key]
        path = bucket_path(self.root, start, end)
        obj = load_json_if_exists(path, {})
        self._cache[key] = obj
        self._dirty[key] = False
        return obj

    def update_entry(self, uid: str, latest_obj: Dict[str, Any], page: int):
        rank = 0
        try:
            rank = int(latest_obj.get("rank", 0) or 0)
        except Exception:
            rank = 0
        start, end = rank_bucket_bounds(rank)
        key = (start, end)
        bucket = self._load_bucket(start, end)
        existing = bucket.get(uid, {})
        pages = existing.get("pages", [])
        if page not in pages:
            pages = pages + [page]
        bucket[uid] = {"latest": latest_obj, "pages": pages}
        self._dirty[key] = True

    def save_dirty(self):
        for key, dirty in list(self._dirty.items()):
            if not dirty:
                continue
            start, end = key
            path = bucket_path(self.root, start, end)
            atomic_write(path, self._cache.get(key, {}))
            self._dirty[key] = False

    def force_save_all(self):
        for key in list(self._cache.keys()):
            path = bucket_path(self.root, key[0], key[1])
            atomic_write(path, self._cache.get(key, {}))
            self._dirty[key] = False

def graceful(sig, frame):
    global _stop
    _stop = True

signal.signal(signal.SIGINT, graceful)
signal.signal(signal.SIGTERM, graceful)

def run(server_key: str):
    global _last_save, _stop
    base = HOSTNAMES[server_key]
    outdir = os.path.join("Data", server_key)
    ensure_dir = lambda p: os.makedirs(p, exist_ok=True)
    ensure_dir(outdir)
    last_path = os.path.join(outdir, "last.json")
    last = load_json_if_exists(last_path, {"page": 1})
    start_page = last.get("page", 1)
    if start_page <= 0:
        start_page = 1
    page = start_page
    total_expected = None
    buckets = BucketManager(outdir)

    while True:
        if _stop:
            last["page"] = page
            atomic_write(last_path, last)
            buckets.force_save_all()
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
            buckets.force_save_all()
            break

        for ent in data_array:
            uid = normalize_id(ent)
            latest = dict(ent)
            if "history" in latest:
                latest.pop("history", None)
            buckets.update_entry(uid, latest, page)

        last["page"] = page + 1
        atomic_write(last_path, last)

        if total_expected:
            collected = 0
            for b in buckets._cache.values():
                collected += len(b)
            try:
                pct = (collected * 100.0) / int(total_expected)
            except Exception:
                pct = 0.0
            print(f"{pct:.6f}% page={page} collected={collected} total={total_expected}")
        else:
            collected = sum(len(b) for b in buckets._cache.values())
            print(f"page={page} collected={collected}")

        page += 1
        now = time.time()
        if now - _last_save > SAVE_INTERVAL:
            buckets.save_dirty()
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
