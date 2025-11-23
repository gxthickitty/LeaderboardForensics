#!/usr/bin/env python3
import sys
import os
import json
import re
from pathlib import Path
from collections import defaultdict
from typing import Optional, List, Dict, Any
from statistics import median
from difflib import SequenceMatcher

try:
    from unidecode import unidecode
except Exception:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "Unidecode"])
    from unidecode import unidecode

MIN_GROUP_SIZE = 3
NAME_SIMILARITY_THRESHOLD = 0.70
SCORE_TOLERANCE_RATIO = 0.30
MAX_FILENAME_LEN = 64
LEET_MODE = True

LEET_SUBS = {
    '4': 'a', '@': 'a', '8': 'b', '3': 'e', '6': 'g', '9': 'g', '1': 'l', '!': 'i',
    '0': 'o', '5': 's', '$': 's', '7': 't', '+': 't', '2': 'z'
}

HOMOGLYPHS = {
    '฿': 'b', '€': 'e', '£': 'l', '₩': 'w', '¥': 'y', '©': 'c', '®': 'r'
}

def find_data_www(start_path: Optional[Path] = None) -> Optional[Path]:
    if start_path is None:
        start_path = Path.cwd()
    for base in [start_path] + list(start_path.parents):
        candidate = base / "data" / "www"
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve()
    max_walk = 3
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
        if depth > max_walk:
            dirs[:] = []
    return None

def collapse_repeats(s: str) -> str:
    return re.sub(r'(.)\1{2,}', r'\1\1', s)

def remove_separators(s: str) -> str:
    return re.sub(r'[\s\-_\.]+', '', s)

def replace_homoglyphs(s: str) -> str:
    for k, v in HOMOGLYPHS.items():
        s = s.replace(k, v)
    return s

def de_leet(s: str) -> str:
    result = []
    for ch in s:
        low = ch.lower()
        if low in LEET_SUBS:
            result.append(LEET_SUBS[low])
        else:
            result.append(low)
    return ''.join(result)

def normalize_name_variants(raw: str, leet: bool = LEET_MODE) -> List[str]:
    base = unidecode(raw or "")
    base = replace_homoglyphs(base)
    base = base.lower()
    base = re.sub(r'[^a-z0-9\s\-_\.@!$+]', '', base)
    base = base.strip()
    v1 = re.sub(r'[\s\-_\.@!$+]+', ' ', base).strip()
    v1 = re.sub(r'\s+', ' ', v1)
    v2 = collapse_repeats(v1)
    v3 = remove_separators(v2)
    if leet:
        dl_v1 = de_leet(v1)
        dl_v2 = de_leet(v2)
        dl_v3 = de_leet(v3)
    else:
        dl_v1 = v1
        dl_v2 = v2
        dl_v3 = v3
    stripped_numbers = lambda s: re.sub(r'\d+', '', s)
    variants = [
        v1,
        v2,
        v3,
        dl_v1,
        dl_v2,
        dl_v3,
        stripped_numbers(v1),
        stripped_numbers(v2),
        stripped_numbers(v3),
        stripped_numbers(dl_v1),
        stripped_numbers(dl_v2),
        stripped_numbers(dl_v3)
    ]
    uniq = []
    for x in variants:
        if x not in uniq:
            uniq.append(x)
    cleaned = [re.sub(r'[^a-z0-9]+', '', u) for u in uniq if u is not None]
    return [c for c in cleaned if c]

def name_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

def max_variant_similarity(a_raw: str, b_raw: str) -> float:
    a_vars = normalize_name_variants(a_raw)
    b_vars = normalize_name_variants(b_raw)
    best = 0.0
    for av in a_vars:
        for bv in b_vars:
            sim = name_similarity(av, bv)
            if sim > best:
                best = sim
                if best >= 0.999:
                    return best
    return best

def pairwise_avg_similarity(names: List[str]) -> float:
    if len(names) < 2:
        return 0.0
    total = 0.0
    pairs = 0
    for i in range(len(names)):
        for j in range(i+1, len(names)):
            total += max_variant_similarity(names[i], names[j])
            pairs += 1
    return total / pairs if pairs else 0.0

def numeric_score(val: Any) -> Optional[float]:
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val))
    except Exception:
        return None

def scores_consistent(vals: List[float], tolerance_ratio: float) -> bool:
    if len(vals) < 2:
        return False
    med = median(vals)
    if med == 0:
        return all(abs(v) <= tolerance_ratio for v in vals)
    for v in vals:
        if abs(v - med) / max(abs(med), 1.0) > tolerance_ratio:
            return False
    return True

def sanitize_filename(s: str, maxlen: int = MAX_FILENAME_LEN) -> str:
    s2 = re.sub(r'[^A-Za-z0-9_\-]', '_', s)[:maxlen].strip('_')
    return s2 or "group"

def cluster_similar_users(users: Dict[str, Dict[str, Any]], name_threshold: float, min_group: int) -> List[List[str]]:
    entries = []
    for uid, udata in users.items():
        uname = udata.get("latest", {}).get("username") or ""
        entries.append((uid, uname))
    unassigned = set(uid for uid, _ in entries)
    name_map = {uid: nm for uid, nm in entries}
    clusters = []
    for uid, nm in entries:
        if uid not in unassigned:
            continue
        group = [uid]
        unassigned.remove(uid)
        to_check = list(unassigned)
        for other in to_check:
            sim = max_variant_similarity(nm, name_map[other])
            if sim >= name_threshold:
                group.append(other)
                unassigned.discard(other)
        if len(group) >= min_group:
            clusters.append(group)
    return clusters

def build_group_payload(group_uids: List[str], users: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    accounts = []
    names = []
    scores = []
    for uid in group_uids:
        u = users.get(uid, {})
        uname = u.get("latest", {}).get("username") or ""
        names.append(uname)
        acct = {"user_id": uid, "username": uname}
        sc = numeric_score(u.get("score"))
        if sc is not None:
            acct["score"] = sc
            scores.append(sc)
        acct.update({k: v for k, v in u.items() if k != "latest" and k != "score"})
        accounts.append(acct)
    avg_name_sim = pairwise_avg_similarity(names)
    med_score = median(scores) if scores else None
    score_ok = scores_consistent(scores, SCORE_TOLERANCE_RATIO) if scores else False
    return {
        "usernames": [a["username"] for a in accounts],
        "count": len(accounts),
        "avg_name_similarity": round(avg_name_sim, 3),
        "median_score": med_score,
        "score_consistent": bool(score_ok),
        "accounts": accounts
    }

def process_batches(data_www: Path, hits_root: Path):
    bot_dir = hits_root / "Bot_Recognition"
    collections_dir = hits_root / "suspicious_accounts_collections"
    bot_dir.mkdir(parents=True, exist_ok=True)
    collections_dir.mkdir(parents=True, exist_ok=True)
    all_groups = []
    for entry in sorted([p for p in data_www.iterdir() if p.is_dir()]):
        data_file = entry / "data.json"
        if not data_file.exists():
            continue
        try:
            with data_file.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue
        clusters = cluster_similar_users(data, NAME_SIMILARITY_THRESHOLD, MIN_GROUP_SIZE)
        suspicious_for_batch = []
        for cluster in clusters:
            payload = build_group_payload(cluster, data)
            if payload["count"] < MIN_GROUP_SIZE:
                continue
            if payload["avg_name_similarity"] < NAME_SIMILARITY_THRESHOLD:
                continue
            if not payload["score_consistent"]:
                continue
            suspicious_for_batch.append(payload)
            fn = bot_dir / f"{sanitize_filename(entry.name)}_{sanitize_filename(payload['usernames'][0] or 'group')}.json"
            with fn.open("w", encoding="utf-8") as out:
                json.dump(payload, out, indent=2, ensure_ascii=False)
        if suspicious_for_batch:
            all_groups.extend(suspicious_for_batch)
    combined = hits_root / "suspicious_accounts.json"
    with combined.open("w", encoding="utf-8") as f:
        json.dump({"total": len(all_groups), "groups": all_groups}, f, indent=2, ensure_ascii=False)
    for idx, g in enumerate(all_groups, 1):
        name = f"group_{idx}_{sanitize_filename(g['usernames'][0] or 'group')}"
        path = collections_dir / f"{name}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump(g, f, indent=2, ensure_ascii=False)
    print(f"Finished. Created {len(all_groups)} groups. Output written to {hits_root}")

def main():
    data_www = find_data_www()
    if not data_www:
        print("data/www not found. Run from project root or place script inside project.")
        sys.exit(1)
    hits_root = (data_www.parent / "Hits").resolve()
    hits_root.mkdir(parents=True, exist_ok=True)
    process_batches(data_www, hits_root)

if __name__ == "__main__":
    main()
