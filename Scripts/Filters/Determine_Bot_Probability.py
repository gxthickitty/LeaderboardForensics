#!/usr/bin/env python3
import os
import sys
import json
import re
from pathlib import Path
from collections import defaultdict
from typing import Optional, List, Tuple, Dict
try:
    from unidecode import unidecode
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "unidecode"])
    from unidecode import unidecode

def find_data_www(start_path: Optional[Path] = None) -> Optional[Path]:
    if start_path is None:
        start_path = Path.cwd()
    search_candidates = [start_path] + list(start_path.parents)
    for base in search_candidates:
        d = base / "data" / "www"
        if d.exists() and d.is_dir():
            return d.resolve()
    max_walk_depth = 3
    for root, dirs, files in os.walk(start_path):
        parts = Path(root).parts
        if "data" in parts and "www" in parts:
            p = Path(root)
            if p.name == "www" and p.parent.name == "data":
                return p.resolve()
        try:
            rel = Path(root).relative_to(start_path)
            depth = len(rel.parts)
        except Exception:
            depth = 0
        if depth > max_walk_depth:
            dirs[:] = []
    return None

def normalize_username(username: str) -> str:
    if not username:
        return ""
    normalized = unidecode(username)
    cleaned = re.sub(r"[^\w\s@.\-]", "", normalized)
    return cleaned.lower()

BOT_KEYWORDS_RE = re.compile(r"bot|farm|xp|auto|afk|macro|grind|gold|seller", re.I)
SEQUENTIAL_RE = re.compile(r"012|123|234|345|456|567|678|789|987|876|765|654|543|432|321|210")
REPEATED_RE = re.compile(r"(\w{2,})\1{2,}")
CONSEC_DUP_RE = re.compile(r"(.)\1{3,}")

def detect_patterns(username: str, normalized: str) -> Dict[str, bool]:
    return {
        "ends_with_numbers": bool(re.search(r"\d{2,}$", username)),
        "contains_bot_keywords": bool(BOT_KEYWORDS_RE.search(normalized)),
        "repeated_patterns": bool(REPEATED_RE.search(normalized)),
        "sequential_numbers": bool(SEQUENTIAL_RE.search(username)),
        "many_numbers": len(re.findall(r"\d", username)) >= 4,
        "special_word_combos": bool(re.search(r"(farm|bot|auto).*\d|\d.*(farm|bot|auto)", normalized, re.I)),
        "generic_names": bool(re.search(r"^(player|user|account|test)\d+$", normalized, re.I)),
        "consecutive_duplicates": bool(CONSEC_DUP_RE.search(normalized))
    }

def strict_base_from_username(username: str) -> str:
    s = unidecode(username or "").lower()
    s = re.sub(r'[\s\-_\.]+', ' ', s).strip()
    s = re.sub(r'[^a-z0-9 ]+', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'\b(bot|farm|auto|afk|macro|grind|gold|seller|account|player|user|test)\b$', '', s).strip()
    s = re.sub(r'\d+$', '', s).strip()
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-z0-9_]+', '', s)
    if len(s) < 2:
        s = re.sub(r'[^a-z0-9]+', '', unidecode(username or "").lower())[:6]
    return s or "group"

def find_similar_usernames_strict(users_data: Dict[str, dict]) -> Dict[str, List[Tuple[str, str]]]:
    buckets = defaultdict(list)
    for uid, data in users_data.items():
        username = data.get("latest", {}).get("username", "") or ""
        base = strict_base_from_username(username)
        if len(base) >= 2:
            buckets[base].append((uid, username))
    out = {}
    for base, lst in buckets.items():
        if len(lst) >= 2:
            out[base] = lst
    return out

def analyze_user(uid: str, user_data: dict) -> dict:
    username = user_data.get("latest", {}).get("username") or ""
    normalized = normalize_username(username)
    patterns = detect_patterns(username, normalized)
    weights = {
        "contains_bot_keywords": 2,
        "special_word_combos": 2,
        "sequential_numbers": 2,
        "ends_with_numbers": 1,
        "repeated_patterns": 1,
        "many_numbers": 1,
        "generic_names": 1,
        "consecutive_duplicates": 1
    }
    score = sum(weights.get(k, 1) for k, v in patterns.items() if v)
    return {
        "user_id": uid,
        "username": username,
        "normalized_username": normalized,
        "suspicion_score": int(score),
        "patterns": patterns,
        "user_data": user_data
    }

def process_batch(file_path: Path, threshold: int = 3) -> List[dict]:
    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    suspicious = []
    for uid, userdata in data.items():
        res = analyze_user(uid, userdata)
        if res["suspicion_score"] >= threshold:
            suspicious.append(res)
    similar = find_similar_usernames_strict(data)
    for base, users in similar.items():
        for uid, uname in users:
            if not any(u["user_id"] == uid for u in suspicious):
                userdata = data.get(uid, {})
                res = analyze_user(uid, userdata)
                res["similar_group"] = base
                res["similar_users"] = [u for _, u in users]
                suspicious.append(res)
    return suspicious

def sanitize_filename(s: str, maxlen: int = 64) -> str:
    s = re.sub(r'[^a-zA-Z0-9_\-]', '_', s)[:maxlen].strip('_')
    return s or "group"

def group_suspicious_collections_strict(all_suspicious: List[dict], collections_dir: Path):
    by_base = defaultdict(list)
    for item in all_suspicious:
        base = item.get("similar_group") or strict_base_from_username(item.get("username") or item.get("normalized_username") or "")
        by_base[base].append(item)
    collections_dir.mkdir(parents=True, exist_ok=True)
    for base, items in by_base.items():
        if len(items) < 2:
            continue
        fn = collections_dir / f"base_{sanitize_filename(base)}.json"
        with fn.open("w", encoding="utf-8") as f:
            json.dump({"count": len(items), "accounts": items}, f, indent=2, ensure_ascii=False)

def main():
    data_www = find_data_www()
    if not data_www:
        print("Could not locate data/www. Place this script inside the project or run it from project root.")
        sys.exit(1)
    hits_root = (data_www.parent / "Hits").resolve()
    bot_dir = hits_root / "Bot_Recognition"
    collections_root = hits_root / "suspicious_accounts_collections"
    bot_dir.mkdir(parents=True, exist_ok=True)
    collections_root.mkdir(parents=True, exist_ok=True)
    all_suspicious = []
    for entry in sorted(data_www.iterdir()):
        if not entry.is_dir():
            continue
        data_file = entry / "data.json"
        if not data_file.exists():
            continue
        suspicious = process_batch(data_file)
        for s in suspicious:
            s.setdefault("user_data", {})
            s["user_data"].setdefault("meta", {})["batch"] = entry.name
        if suspicious:
            out = bot_dir / f"{sanitize_filename(entry.name)}_bot.json"
            with out.open("w", encoding="utf-8") as f:
                json.dump(suspicious, f, indent=2, ensure_ascii=False)
            all_suspicious.extend(suspicious)
    combined_bot = hits_root / "suspicious_accounts.json"
    with combined_bot.open("w", encoding="utf-8") as f:
        json.dump({"total": len(all_suspicious), "accounts": all_suspicious}, f, indent=2, ensure_ascii=False)
    group_suspicious_collections_strict(all_suspicious, collections_root)
    print("Done. Found {} suspicious accounts.".format(len(all_suspicious)))
    print("Hits written to {}".format(hits_root))

if __name__ == "__main__":
    main()
