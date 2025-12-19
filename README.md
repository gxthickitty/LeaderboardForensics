# KoGaMa Leaderboard Intelligence System

A high-performance, two-component system written in Go for large-scale collection and analysis of KoGaMa leaderboard data.  
This version supersedes the earlier Python implementation and prioritizes throughput, crash safety, and long-running stability while maintaining strict data integrity guarantees.

The system consists of:
- a concurrent leaderboard data collector
- an advanced username analysis engine with leetspeak and obfuscation detection

---

## 🏗️ System Architecture

### **Leaderboard Data Collector**

**Purpose:**  
Continuously scrape KoGaMa leaderboard data with concurrency, resumability, and atomic persistence.

**Core Features:**
- Multi-server support: `www`, `br`, `friends`
- Worker-pool based concurrent fetching
- Retry logic with backoff for network and rate-limit resilience
- Automatic resume from last processed page
- Graceful shutdown via SIGINT and SIGTERM
- Atomic JSON writes to prevent corruption
- Rank-bucketed storage to control memory usage

**Storage Strategy:**
```
Data/
├── www/
│   ├── 1to20000/
│   │   └── data.json
│   ├── 20001to40000/
│   │   └── data.json
│   └── last.json
├── br/
└── friends/
```

Each bucket stores:
- `latest`: most recent snapshot of the account
- `pages`: leaderboard pages on which the account appeared

**Key Components:**
- `BucketManager`: manages rank-range buckets and dirty state
- `RetryClient`: HTTP client with retry and backoff
- `atomicWrite`: crash-safe JSON persistence
- `normalizeID`: resolves differing ID field names

**Runtime Flow:**
1. Load last saved page
2. Spawn worker goroutines
3. Fetch leaderboard pages concurrently
4. Normalize entries and assign to rank buckets
5. Periodically flush dirty buckets to disk
6. Persist progress on shutdown or timed intervals

**Usage:**
```
go run 
Enter server [br,www,friends]: www
```

---

### **Username Analysis Engine**

**Purpose:**  
Scan collected leaderboard data for inappropriate usernames using aggressive normalization and leetspeak-aware pattern matching.

**Detection Capabilities:**
- Configurable leetspeak expansion
- Unicode normalization and diacritic stripping
- Case-insensitive matching
- Separator-agnostic character matching
- Multiple normalized variants per username
- Boundary-safe detection to prevent partial matches

**Normalization Pipeline:**
1. Unicode NFD normalization  
2. Diacritic removal  
3. ASCII folding  
4. Lowercasing  
5. Symbol and separator collapsing  

**Pattern Construction:**
Each slur is converted into a compiled regular expression that:
- allows arbitrary separators between characters
- matches common leetspeak substitutions
- enforces non-alphanumeric boundaries

**Conceptual Matching Examples:**
For the slur `test`, the engine will detect:
- `t3s+`
- `t.e_s.t`
- `7€5t`
- `TEST`

while avoiding unrelated substrings inside longer words.

**Output Structure:**
```
Hits/
├── inappropriate_accounts.txt
├── Inappropriate_words/
│   ├── 1to20000_slurs.txt
│   └── 20001to40000_slurs.txt
└── inappropriate_accounts_collections/
    └── txt/
        ├── slur_example.txt
        ├── slur_test.txt
        └── slur_word.txt
```

**Output Guarantees:**
- Deterministic results per run
- One entry per detected account
- Stable profile URLs
- Aggregated collections per detected term

---

## 🚀 Quick Start

### Prerequisites
- Go 1.21 or newer
- Network access to KoGaMa APIs

### Step 1: Collect Leaderboard Data
```
go run LeaderboardScraper.go
```

The process may run indefinitely. It can be safely interrupted at any time with Ctrl+C and resumed later.

### Step 2: Prepare Filtering Rules
Create a `flags.json` file in the project root:
```
{
  "explicit": ["slur1", "slur2"],
  "derogatory": ["term1", "term2"],
  "phrases": ["multi word term"]
}
```

### Step 3: Analyze Usernames
```
go run
```

Example output:
```
Done. Found 847 accounts with slurs.
TXT hits written to Data/Hits
```

---

## 🔧 Configuration

### `LeaderboardScraper.go` Constants
```
HOSTNAMES = {
    "www":     "https://www.kogama.com/",
    "br":      "https://www.kogama.com.br/",
    "friends": "https://friends.kogama.com/"
}

COUNT           = 400
BUCKET_SIZE     = 20000
WORKERS         = 6
PREFETCH_PAGES  = 12
SAVE_INTERVAL   = 30s
REQUEST_TIMEOUT = 10s
```

### `LeaderboardForensics.go` Customization
- Extend `LEET_TABLE` for additional substitutions
- Adjust filename sanitization rules for OS compatibility
- Modify minimum slur length via filtering logic

---

## 📊 Data Integrity Guarantees

1. Atomic file writes prevent partial corruption  
2. Incremental saves minimize data loss  
3. Fully resumable scraping sessions  
4. Rank-bucket partitioning limits memory pressure  
5. Deterministic processing for auditability  

---

## 🤝 Credits & Acknowledgments

### Core Development
- **Simon** - system architecture, Go implementation, filter design

### Inspiration
- **Zpayer** (GitHub: RandomUser15456) - original KoGaMa-Search concept

### Moderation & Verification
- **Ejota** - false-positive validation, reporting channel
- **Its_flrwn** - false-positive validation, reporting channel

### Localization Contributors
- Portuguese: <ins> Ejotas </ins>
- Spanish: <ins> Ejotas </ins>
- French:  <ins> Its_flrwn </ins>
- Dutch:  <ins> Kenley193 </ins>
- Croatian:  <ins> TunA </ins>
- Bosnian: <ins> TunA </ins>
- Serbian: <ins> TunA, Raptor </ins> 
- Slovenian: <ins> erm_what_the_shoto  </ins> 

---

### Verification Timeline
| Date | Server | Action | Accounts Affected |
|------|--------|--------|-------------------|
| 2025-11-23 | www | Bot Account Termination | 580 |
| 2025-11-25 | www | Inappropriate Username Account Termination | 700 |
| 2025-12-03 | www | Inappropriate Username Account Termination | 1,400 |

---


## ⚠️ Ethical Use & Disclaimer

### Intended Use
This system is intended exclusively for:
- Platform moderation support
- Community safety research
- Behavioral pattern analysis
- Academic study of online systems

### Prohibited Use
Do not use this system for:
- Harassment or targeting
- Unauthorized surveillance
- Personal data exploitation
- Any activity violating platform rules or laws

### Legal Notice
The authors assume **no responsibility** for misuse.  
Users are solely responsible for compliance with:
- Local and international law
- Platform Terms of Service
- Data protection regulations
- Ethical research standards

---

## 🔄 Project Philosophy

This project prioritizes **data integrity and reproducibility** over stylistic minimalism.

Design principles:
- Resilience against interruption
- Transparent data structures
- Audit-friendly persistence
- Future-proof raw data retention

The guiding approach is **collect now, analyze later**, ensuring long-term analytical flexibility.

---

*Last updated: 19th December 2025*  
*Version: 2.0 (Go rewrite)*  
*Maintainer: Simon*
