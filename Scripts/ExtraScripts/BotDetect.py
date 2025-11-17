
#!/usr/bin/env python3
import sys
import os
import json
import re
from collections import defaultdict
try:
    from unidecode import unidecode
except ImportError:
    print("Installing unidecode...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "unidecode"])
    from unidecode import unidecode

def normalize_username(username):
    if not username:
        return ""
    
    normalized = unidecode(username)
    cleaned = re.sub(r'[^\w\s@.-]', '', normalized)
    
    return cleaned.lower()

def detect_patterns(username, normalized_username):
    patterns = {
        'ends_with_numbers': bool(re.search(r'\d{2,}$', username)),
        'contains_bot_keywords': bool(re.search(r'bot|farm|xp|auto|afk|macro|grind|gold|seller', normalized_username, re.I)),
        'repeated_patterns': bool(re.search(r'(\w{2,})\1{2,}', normalized_username)),
        'sequential_numbers': bool(re.search(r'(012|123|234|345|456|567|678|789|987|876|765|654|543|432|321|210)', username)),
        'many_numbers': len(re.findall(r'\d', username)) >= 4,
        'special_word_combos': bool(re.search(r'(farm|bot|auto).*\d|\d.*(farm|bot|auto)', normalized_username, re.I)),
        'generic_names': bool(re.search(r'^(player|user|account|test)\d+$', normalized_username, re.I)),
        'consecutive_duplicates': bool(re.search(r'(.)\1{3,}', normalized_username)),  # aaaa, 1111, etc.
    }
    return patterns

def check_inappropriate_words(username, normalized_username):
    inappropriate_words = {
        'bot', 'farm', 'grinder', 'auto', 'macro', 'script', 'cheat', 'hack',
        'exploit', 'goldfarmer', 'botted', 'afkfarm'
    }
    
    suspicious_terms = {
        'xp', 'farm', 'bot', 'auto', 'macro', 'grind', 'afk', 'gold', 'item',
        'seller', 'buyer', 'trader', 'account', 'power', 'level', 'exp', 'experience',
        'farmer', 'grinding', 'autofarm'
    }
    
    words_in_name = set(re.findall(r'\w+', normalized_username.lower()))
    
    return {
        'has_inappropriate': bool(words_in_name & inappropriate_words),
        'has_suspicious_terms': bool(words_in_name & suspicious_terms),
        'found_words': list(words_in_name & (inappropriate_words | suspicious_terms))
    }

def find_similar_usernames(users_data):
    base_names = defaultdict(list)
    
    for user_id, data in users_data.items():
        username = data['latest']['username']
        normalized = normalize_username(username)
        base_name = re.sub(r'\d+$', '', normalized)
        base_name = re.sub(r'[_-]*(bot|farm|auto|xp|farmbot|autofarm)$', '', base_name)
        
        if len(base_name) >= 3:
            base_names[base_name].append((user_id, username))
    similar_groups = {}
    for base_name, user_list in base_names.items():
        if len(user_list) >= 2 and len(base_name) >= 3:
            has_number_pattern = any(re.search(r'\d', username) for _, username in user_list)
            if has_number_pattern or len(user_list) >= 3:
                similar_groups[base_name] = user_list
    
    return similar_groups

def analyze_user(user_id, user_data):
    username = user_data['latest']['username']
    normalized = normalize_username(username)
    
    patterns = detect_patterns(username, normalized)
    content_check = check_inappropriate_words(username, normalized)
    score_weights = {
        'contains_bot_keywords': 2,
        'special_word_combos': 2,
        'ends_with_numbers': 1,
        'repeated_patterns': 1,
        'sequential_numbers': 2,
        'many_numbers': 1,
        'generic_names': 1,
        'consecutive_duplicates': 1
    }
    
    suspicion_score = sum(score_weights.get(k, 1) for k, v in patterns.items() if v)
    suspicion_score += 2 if content_check['has_inappropriate'] else 0
    suspicion_score += 1 if content_check['has_suspicious_terms'] else 0
    
    return {
        'user_id': user_id,
        'username': username,
        'normalized_username': normalized,
        'suspicion_score': suspicion_score,
        'patterns': patterns,
        'content_check': content_check,
        'user_data': user_data
    }

def main():
    input_file = input("Path to cleaned JSON file: ").strip()
    
    if not os.path.isfile(input_file):
        print("File not found!")
        return
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"Loaded {len(data)} users for analysis...")
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return
    
    suspicious_users = []
    analysis_results = {}
    
    print("Analyzing usernames for bot patterns...")
    
    for i, (user_id, user_data) in enumerate(data.items()):
        result = analyze_user(user_id, user_data)
        analysis_results[user_id] = result
        
        if result['suspicion_score'] >= 3:
            suspicious_users.append(result)
        
        if (i + 1) % 1000 == 0:
            print(f"Processed {i + 1} users...")
    
    print("Looking for similar username patterns...")
    similar_groups = find_similar_usernames(data)
    
    for base_name, user_list in similar_groups.items():
        for user_id, username in user_list:
            if not any(u['user_id'] == user_id for u in suspicious_users):
                user_data = data[user_id]
                result = analyze_user(user_id, user_data)
                result['similar_group'] = base_name
                result['similar_users'] = [u[1] for u in user_list]
                suspicious_users.append(result)
    
    suspicious_users.sort(key=lambda x: x['suspicion_score'], reverse=True)
    
    hits_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hits')
    os.makedirs(hits_dir, exist_ok=True)
    
    output_data = {
        'summary': {
            'total_users_analyzed': len(data),
            'suspicious_users_found': len(suspicious_users),
            'similar_groups_found': len(similar_groups),
            'detection_threshold': 3
        },
        'suspicious_accounts': suspicious_users,
        'similar_username_groups': similar_groups
    }
    
    output_file = os.path.join(hits_dir, 'suspicious_accounts.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    simplified_output = {}
    for user in suspicious_users:
        simplified_output[user['user_id']] = user['user_data']
    
    simplified_file = os.path.join(hits_dir, 'suspicious_accounts_data_only.json')
    with open(simplified_file, 'w', encoding='utf-8') as f:
        json.dump(simplified_output, f, indent=2, ensure_ascii=False)
    
    print(f"\n=== ANALYSIS COMPLETE ===")
    print(f"Total users analyzed: {len(data)}")
    print(f"Suspicious accounts found: {len(suspicious_users)}")
    print(f"Similar username groups: {len(similar_groups)}")
    
    if suspicious_users:
        print(f"\nTop 10 most suspicious accounts:")
        for i, user in enumerate(suspicious_users[:10]):
            patterns_found = [k for k, v in user['patterns'].items() if v]
            print(f"  {i+1}. {user['username']} (score: {user['suspicion_score']})")
            if patterns_found:
                print(f"     Patterns: {', '.join(patterns_found)}")
    
    if similar_groups:
        print(f"\nTop similar username groups:")
        for base_name, user_list in list(similar_groups.items())[:5]:
            print(f"  '{base_name}': {[u[1] for u in user_list]}")
    
    print(f"\nResults saved to:")
    print(f"  - {output_file} (detailed analysis)")
    print(f"  - {simplified_file} (clean user data only)")

if __name__ == "__main__":
    main()
