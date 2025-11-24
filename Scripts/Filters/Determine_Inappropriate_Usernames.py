#!/usr/bin/env python3
import os
import sys
import json
import re
from pathlib import Path
from collections import defaultdict
from typing import Optional, Iterable, Tuple, Dict, Set

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
    "rape",
    "raped",
    "black",
    "monkey",
    "horny",
    "sex",
    "s3x"
]

LEET_TABLE = {
    "a": ["a", "4", "@", "ª"],
    "b": ["b", "8", "6"],
    "c": ["c", "<", "(", "{", "[", "¢"],
    "d": ["d"],
    "e": ["e", "3", "€"],
    "f": ["f"],
    "g": ["g", "9", "6"],
    "h": ["h", "#"],
    "i": ["i", "1", "!", "l", "|"],
    "j": ["j"],
    "k": ["k"],
    "l": ["l", "1", "|", "¡"],
    "m": ["m"],
    "n": ["n"],
    "o": ["o", "0", "()"],
    "p": ["p"],
    "q": ["q", "9"],
    "r": ["r"],
    "s": ["s", "5", "$"],
    "t": ["t", "7", "+"],
    "u": ["u", "v"],
    "v": ["v", "\\/"],
    "w": ["w", "\\/\\/"],
    "x": ["x", "%", "*"],
    "y": ["y"],
    "z": ["z", "2"]
}

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

def fetch_slurs() -> Set[str]:
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
                    s = unidecode(w).strip().lower()
                    s = re.sub(r'[^a-z0-9\s\-_.]', '', s)
                    if s:
                        aggregated.add(s)
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

def sanitize_for_matching(text: str) -> str:
    if not text:
        return ""
    text = unidecode(text)
    text = text.lower()
    return text

def build_slur_pattern(slur: str, l33t_mode: bool = True) -> re.Pattern:
    base = re.sub(r'[^a-z0-9]', '', slur.lower())
    if not base:
        base = slur.lower()
    parts = []
    for ch in base:
        if l33t_mode and ch in LEET_TABLE:
            variants = LEET_TABLE[ch]
            variants_escaped = [re.escape(v) for v in sorted(set(variants), key=len, reverse=True)]
            char_class = "(?:" + "|".join(variants_escaped) + "|" + re.escape(ch) + ")"
        else:
            char_class = re.escape(ch)
        parts.append(char_class)
    sep = r"[\W_]*"
    pattern = sep.join(parts)
    pattern = r"(?<![a-z0-9])" + pattern + r"(?![a-z0-9])"
    return re.compile(pattern, re.I)

def compile_slur_patterns(slurs: Iterable[str], l33t_mode: bool = True) -> Dict[str, re.Pattern]:
    out = {}
    for s in slurs:
        key = re.sub(r'[^a-z0-9]+', '', s.lower())
        if not key:
            continue
        out[key] = build_slur_pattern(key, l33t_mode=l33t_mode)
    return out

def iter_username_candidates(raw_username: str) -> Iterable[str]:
    yield raw_username
    normalized = sanitize_for_matching(raw_username)
    yield normalized
    collapsed = re.sub(r'[\W_]+', '', normalized)
    if collapsed != normalized:
        yield collapsed
    spaced_removed = re.sub(r'\s+', '', normalized)
    if spaced_removed != collapsed:
        yield spaced_removed

def detect_slurs_in_username(username: str, patterns: Dict[str, re.Pattern]) -> Set[str]:
    found = set()
    candidates = list(iter_username_candidates(username))
    for cand in candidates:
        for key, pat in patterns.items():
            if pat.search(cand):
                found.add(key)
    return found

def process_batch_file(path: Path, patterns: Dict[str, re.Pattern]) -> Tuple[list, list]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return [], []
    flagged = []
    details = []
    for uid, userdata in data.items():
        username = (userdata.get("latest", {}) or {}).get("username") or ""
        if not username:
            continue
        found = detect_slurs_in_username(username, patterns)
        if found:
            record = {
                "user_id": uid,
                "username": username,
                "found_slurs": sorted(found),
                "user_data": userdata
            }
            flagged.append(record)
            for sl in found:
                details.append((sl, record))
    return flagged, details

def sanitize_filename(s: str, maxlen: int = 64) -> str:
    s = re.sub(r'[^a-zA-Z0-9_\-]', '_', s)[:maxlen].strip('_')
    return s or "group"

def group_slur_collections(all_flagged: list, collections_dir: Path):
    by_slur = defaultdict(list)
    for item in all_flagged:
        for s in item.get("found_slurs", []):
            by_slur[s].append(item)
    collections_dir.mkdir(parents=True, exist_ok=True)
    for slur, items in by_slur.items():
        fn = collections_dir / f"slur_{sanitize_filename(slur)}.json"
        with fn.open("w", encoding="utf-8") as f:
            json.dump({"count": len(items), "accounts": items}, f, indent=2, ensure_ascii=False)

def main():
    data_www = find_data_www()
    if not data_www:
        print("Could not locate data/www. Place this script inside the project or run it from project root.")
        sys.exit(1)
    hits_root = (data_www.parent / "Hits").resolve()
    slur_dir = hits_root / "Inappropriate_words"
    slur_collections_root = hits_root / "inappropriate_accounts_collections"
    slur_dir.mkdir(parents=True, exist_ok=True)
    slur_collections_root.mkdir(parents=True, exist_ok=True)
    slurs = fetch_slurs()
    patterns = compile_slur_patterns(slurs, l33t_mode=True)
    all_flagged = []
    for entry in sorted(data_www.iterdir()):
        if not entry.is_dir():
            continue
        data_file = entry / "data.json"
        if not data_file.exists():
            continue
        flagged, details = process_batch_file(data_file, patterns)
        for f in flagged:
            f.setdefault("user_data", {})
            f["user_data"].setdefault("meta", {})["batch"] = entry.name
        if flagged:
            out = slur_dir / f"{sanitize_filename(entry.name)}_slurs.json"
            with out.open("w", encoding="utf-8") as f:
                json.dump(flagged, f, indent=2, ensure_ascii=False)
            all_flagged.extend(flagged)
    combined_slur = hits_root / "inappropriate_accounts.json"
    with combined_slur.open("w", encoding="utf-8") as f:
        json.dump({"total": len(all_flagged), "accounts": all_flagged}, f, indent=2, ensure_ascii=False)
    group_slur_collections(all_flagged, slur_collections_root)
    print("Done. Found {} accounts with slurs.".format(len(all_flagged)))
    print("Hits written to {}".format(hits_root))

if __name__ == "__main__":
    main()