#!/usr/bin/env python3
import os
import sys
import json
import re
from pathlib import Path
from collections import defaultdict
try:
    from unidecode import unidecode
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "unidecode"])
    from unidecode import unidecode
try:
    import requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

SLUR_SOURCES = [
    "https://raw.githubusercontent.com/punyajoy/Fearspeech-project/refs/heads/main/slur_keywords.json",
    "https://raw.githubusercontent.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words/master/en"
]

MINIMAL_FALLBACK = [
    "gay",
    "nigger",
    "niger",
    "nigga",
    "nga",
    "ngga",
    "rape",
    "raped",
    "black",
    "monkey",
    "horny",
    "sex",
    "s3x"
]

def find_data_www(start_path: Path = None) -> Path | None:
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

def fetch_slurs() -> set:
    session = requests.Session()
    session.headers.update({"User-Agent": "slur-fetcher/1.0"})
    aggregated = set()
    for url in SLUR_SOURCES:
        try:
            r = session.get(url, timeout=12)
            r.raise_for_status()
            text = r.text
            items = []
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    for v in parsed.values():
                        if isinstance(v, list):
                            items.extend(v)
                        elif isinstance(v, dict):
                            for inner in v.values():
                                if isinstance(inner, list):
                                    items.extend(inner)
                                elif isinstance(inner, str):
                                    items.append(inner)
                        elif isinstance(v, str):
                            items.append(v)
                elif isinstance(parsed, list):
                    items = parsed
                elif isinstance(parsed, str):
                    lines = [l.strip() for l in parsed.splitlines() if l.strip()]
                    items = lines
            except Exception:
                lines = [l.strip() for l in text.splitlines() if l.strip()]
                items = lines
            for w in items:
                if isinstance(w, str) and w.strip():
                    aggregated.add(unidecode(w).strip().lower())
        except Exception:
            continue
    if not aggregated:
        aggregated.update(MINIMAL_FALLBACK)
    normalized = set()
    for s in aggregated:
        cleaned = re.sub(r'[^a-z0-9]+', '', s.lower())
        if len(cleaned) >= 2:
            normalized.add(cleaned)
    return normalized

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

def detect_patterns(username: str, normalized: str) -> dict:
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

def check_inappropriate_words(normalized: str, slurs: set) -> dict:
    words = set(re.findall(r"\w+", normalized.lower()))
    found = set()
    for w in words:
        cleaned = re.sub(r'[^a-z0-9]+', '', w.lower())
        if cleaned in slurs:
            found.add(cleaned)
    return {"has_inappropriate": bool(found), "found_words": sorted(found)}

def find_similar_usernames(users_data: dict) -> dict:
    base_names = defaultdict(list)
    for uid, data in users_data.items():
        username = data.get("latest", {}).get("username", "") or ""
        normalized = normalize_username(username)
        base = re.sub(r"\d+$", "", normalized)
        base = re.sub(r"[_\-]*(bot|farm|auto|xp|farmbot|autofarm)$", "", base)
        if len(base) >= 3:
            base_names[base].append((uid, username))
    out = {}
    for base, lst in base_names.items():
        if len(lst) >= 2:
            has_digit = any(re.search(r"\d", name) for _, name in lst)
            if has_digit or len(lst) >= 3:
                out[base] = lst
    return out

def analyze_user(uid: str, user_data: dict, slurs: set) -> dict:
    username = user_data.get("latest", {}).get("username") or ""
    normalized = normalize_username(username)
    patterns = detect_patterns(username, normalized)
    content_check = check_inappropriate_words(normalized, slurs)
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
    if content_check["has_inappropriate"]:
        score += 2
    return {
        "user_id": uid,
        "username": username,
        "normalized_username": normalized,
        "suspicion_score": int(score),
        "patterns": patterns,
        "content_check": content_check,
        "user_data": user_data
    }

def process_batch(file_path: Path, slurs: set, threshold: int = 3) -> tuple[list, list]:
    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return [], []
    suspicious = []
    inappropriate = []
    for uid, userdata in data.items():
        res = analyze_user(uid, userdata, slurs)
        if res["suspicion_score"] >= threshold:
            suspicious.append(res)
        if res["content_check"]["has_inappropriate"]:
            inappropriate.append(res)
    similar = find_similar_usernames(data)
    for base, users in similar.items():
        for uid, uname in users:
            if not any(u["user_id"] == uid for u in suspicious):
                userdata = data.get(uid, {})
                res = analyze_user(uid, userdata, slurs)
                res["similar_group"] = base
                res["similar_users"] = [u for _, u in users]
                suspicious.append(res)
    return suspicious, inappropriate

def sanitize_filename(s: str, maxlen: int = 64) -> str:
    s = re.sub(r'[^a-zA-Z0-9_\-]', '_', s)[:maxlen].strip('_')
    return s or "group"

def group_suspicious_collections(all_suspicious: list, collections_dir: Path):
    by_base = defaultdict(list)
    by_pattern = defaultdict(list)
    by_batch = defaultdict(list)
    for item in all_suspicious:
        base = item.get("similar_group") or re.sub(r'\d+$', '', item.get("normalized_username", ""))[:24]
        by_base[base].append(item)
        top_patterns = [k for k, v in item.get("patterns", {}).items() if v]
        for p in top_patterns:
            by_pattern[p].append(item)
        batch = item.get("user_data", {}).get("meta", {}).get("batch") or item.get("user_data", {}).get("batch") or "unknown_batch"
        by_batch[batch].append(item)
    collections_dir.mkdir(parents=True, exist_ok=True)
    for base, items in by_base.items():
        fn = collections_dir / f"base_{sanitize_filename(base)}.json"
        with fn.open("w", encoding="utf-8") as f:
            json.dump({"count": len(items), "accounts": items}, f, indent=2, ensure_ascii=False)
    for pattern, items in by_pattern.items():
        fn = collections_dir / f"pattern_{sanitize_filename(pattern)}.json"
        with fn.open("w", encoding="utf-8") as f:
            json.dump({"count": len(items), "accounts": items}, f, indent=2, ensure_ascii=False)
    for batch, items in by_batch.items():
        fn = collections_dir / f"batch_{sanitize_filename(batch)}.json"
        with fn.open("w", encoding="utf-8") as f:
            json.dump({"count": len(items), "accounts": items}, f, indent=2, ensure_ascii=False)

def group_slur_collections(all_inappropriate: list, slur_collections_dir: Path):
    by_slur = defaultdict(list)
    for item in all_inappropriate:
        found = item.get("content_check", {}).get("found_words", [])
        for s in found:
            by_slur[s].append(item)
    slur_collections_dir.mkdir(parents=True, exist_ok=True)
    for slur, items in by_slur.items():
        fn = slur_collections_dir / f"slur_{sanitize_filename(slur)}.json"
        with fn.open("w", encoding="utf-8") as f:
            json.dump({"count": len(items), "accounts": items}, f, indent=2, ensure_ascii=False)

def main():
    data_www = find_data_www()
    if not data_www:
        print("Could not locate data/www. Place this script inside the project or run it from project root.")
        sys.exit(1)
    hits_root = (data_www.parent / "Hits").resolve()
    bot_dir = hits_root / "Bot_Recognition"
    slur_dir = hits_root / "Inappropriate_words"
    collections_root = hits_root / "suspicious_accounts_collections"
    slur_collections_root = hits_root / "inappropriate_accounts_collections"
    bot_dir.mkdir(parents=True, exist_ok=True)
    slur_dir.mkdir(parents=True, exist_ok=True)
    slurs = fetch_slurs()
    all_suspicious = []
    all_inappropriate = []
    for entry in sorted(data_www.iterdir()):
        if not entry.is_dir():
            continue
        data_file = entry / "data.json"
        if not data_file.exists():
            continue
        suspicious, inappropriate = process_batch(data_file, slurs)
        for s in suspicious:
            s.setdefault("user_data", {})
            s["user_data"].setdefault("meta", {})["batch"] = entry.name
        for a in inappropriate:
            a.setdefault("user_data", {})
            a["user_data"].setdefault("meta", {})["batch"] = entry.name
        if suspicious:
            out = bot_dir / f"{sanitize_filename(entry.name)}_bot.json"
            with out.open("w", encoding="utf-8") as f:
                json.dump(suspicious, f, indent=2, ensure_ascii=False)
            all_suspicious.extend(suspicious)
        if inappropriate:
            out = slur_dir / f"{sanitize_filename(entry.name)}_slurs.json"
            with out.open("w", encoding="utf-8") as f:
                json.dump(inappropriate, f, indent=2, ensure_ascii=False)
            all_inappropriate.extend(inappropriate)
    combined_bot = hits_root / "suspicious_accounts.json"
    combined_slur = hits_root / "inappropriate_accounts.json"
    with combined_bot.open("w", encoding="utf-8") as f:
        json.dump({"total": len(all_suspicious), "accounts": all_suspicious}, f, indent=2, ensure_ascii=False)
    with combined_slur.open("w", encoding="utf-8") as f:
        json.dump({"total": len(all_inappropriate), "accounts": all_inappropriate}, f, indent=2, ensure_ascii=False)
    group_suspicious_collections(all_suspicious, collections_root)
    group_slur_collections(all_inappropriate, slur_collections_root)
    print(f"Done. Found {len(all_suspicious)} suspicious accounts and {len(all_inappropriate)} inappropriate usernames.")
    print(f"Hits written to {hits_root}")

if __name__ == "__main__":
    main()
