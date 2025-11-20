# LeaderboardDB

A structured repository dedicated to collecting, preserving, and analysing every publicly visible account on [WWW KoGaMa](https://www.kogama.com). Its purpose is to identify indicators of automated activity, coordinated boosting, harassment patterns, and usernames that violate platform standards.  
Flagged accounts are stored to support investigation, reporting, and long-term monitoring.

---

## Overview

This project performs a broad, methodical scrape of KoGaMa’s leaderboard. The intention is to build a traceable, auditable dataset rather than perfect code. The focus lies on recognising patterns such as:

- Automated or scripted behaviour  
- Experience or economy boosting chains  
- Suspicious purchasing paths involving avatars or models  
- Inappropriate or offensive usernames  
- Accounts repurposed for advertising unauthorised files or services  

Completeness and reproducibility outrank elegance, ensuring that all retrieved data remains intact for later evaluation.

---

## API Method

Scraping is performed through the public leaderboard endpoint: ``api/leaderboard/top/``  
Since the endpoint enforces a maximum ``count`` of ``400`` per page, the process relies on sequential pagination. Each page is written to disk immediately for safety and later processing.

Request format:  
``{ENDPOINT}?count={COUNT}&page={PAGE}``

Initial scrape launched: **November 16th, 3:42 AM**.

---

## Catalogue Structure

### `LeaderboardDB/Hits/Bots`
A dedicated directory for accounts confirmed to display traits associated with automated or exploitative activity. These commonly include mid-range boosted levels (approximately 8–22) and behaviours such as:

- Purchasing assets to benefit a primary account  
- Interacting in patterns consistent with market manipulation  
- Machine-like activity repetition  
- External or inappropriate promotional links in profile fields  

This folder acts as a consolidated reference for reporting and reviewing suspicious account clusters.

---

## Notes

For those with access: quality of code is not the objective. Only the outcome and the integrity of the dataset are.
