#!/usr/bin/env python3
import os, io, sys, json, shutil

def prompt(msg, default=None):
    v = input(f"{msg}{(' ['+str(default)+']') if default is not None else ''}: ").strip()
    return v if v else default

def stream_top_level_object(path):
    with open(path, "rb") as f:
        data = f.read().decode("utf-8", errors="surrogatepass")
    i = 0
    n = len(data)
    while i < n and data[i].isspace():
        i += 1
    if i >= n or data[i] != "{":
        raise ValueError("File does not start with a JSON object '{'")
    i += 1
    while i < n:
        while i < n and data[i].isspace():
            i += 1
        if i < n and data[i] == "}":
            return
        if i >= n:
            return
        if data[i] != '"':
            j = data.find('"', i)
            if j == -1:
                return
            i = j
        i += 1
        key_chars = []
        while i < n:
            ch = data[i]
            key_chars.append(ch)
            i += 1
            if ch == '"' and key_chars[-2:] != ['\\', '"']:
                break
            if ch == '\\':
                if i < n:
                    key_chars.append(data[i])
                    i += 1
        key_raw = '"' + ''.join(key_chars[:-1]) + '"'
        try:
            key = json.loads(key_raw)
        except Exception:
            key = key_raw
        while i < n and data[i].isspace():
            i += 1
        if i < n and data[i] == ':':
            i += 1
        else:
            j = data.find(':', i)
            if j == -1:
                return
            i = j + 1
        while i < n and data[i].isspace():
            i += 1
        if i >= n:
            return
        start = i
        ch = data[i]
        if ch == '{' or ch == '[':
            depth = 0
            in_string = False
            escaped = False
            while i < n:
                c = data[i]
                if in_string:
                    if escaped:
                        escaped = False
                    elif c == '\\':
                        escaped = True
                    elif c == '"':
                        in_string = False
                else:
                    if c == '"':
                        in_string = True
                    elif c == '{' or c == '[':
                        depth += 1
                    elif c == '}' or c == ']':
                        depth -= 1
                        if depth == 0:
                            i += 1
                            break
                i += 1
            value_raw = data[start:i]
        else:
            if ch == '"':
                i += 1
                while i < n:
                    c = data[i]
                    i += 1
                    if c == '\\':
                        i += 1
                        continue
                    if c == '"':
                        break
                value_raw = data[start:i]
            else:
                j = i
                while j < n and data[j] not in ",}":
                    j += 1
                value_raw = data[start:j].strip()
                i = j
        while i < n and data[i].isspace():
            i += 1
        if i < n and data[i] == ',':
            i += 1
        yield key, value_raw
    return

def process_and_pretty_write(output_path, entries, indent):
    with open(output_path, "w", encoding="utf-8", newline="\n") as out:
        out.write("{\n")
        first = True
        for key, raw_val in entries:
            try:
                parsed_val = json.loads(raw_val)
                
                if isinstance(parsed_val, dict):
                    if 'history' in parsed_val:
                        del parsed_val['history']
                
                pretty_val = json.dumps(parsed_val, ensure_ascii=False, indent=indent)
                
            except json.JSONDecodeError:
                pretty_val = raw_val
            except Exception as e:
                print(f"Error processing key {key}: {e}")
                pretty_val = raw_val
                
            key_json = json.dumps(key, ensure_ascii=False)
            if not first:
                out.write(",\n")
            first = False
            
            pretty_lines = pretty_val.splitlines()
            out.write(" " * indent + key_json + ": ")
            if len(pretty_lines) == 1:
                out.write(pretty_lines[0])
            else:
                out.write(pretty_lines[0] + "\n")
                for line in pretty_lines[1:]:
                    out.write(" " * (indent * 2) + line + "\n")
            out.flush()
        out.write("\n}\n")

def main():
    path = prompt("Path to JSON file", None)
    if not path or not os.path.isfile(path):
        print("File not found.")
        return
    indent = prompt("Indent spaces (integer)", "2")
    try:
        indent = int(indent)
        if indent < 0:
            indent = 2
    except Exception:
        indent = 2
    backup = prompt("Make backup of original file? (y/n)", "y").lower() in ("y", "yes")
    overwrite = prompt("Overwrite original file? (y/n)", "n").lower() in ("y", "yes")
    if backup:
        bak = path + ".bak"
        shutil.copy2(path, bak)
        print("Backup written to", bak)
    entries = []
    bad_count = 0
    yielded = 0
    for key, raw in stream_top_level_object(path):
        yielded += 1
        try:
            json.loads(raw)
            entries.append((key, raw))
        except Exception:
            try:
                fixed = raw.strip()
                if fixed.endswith(","):
                    fixed = fixed[:-1]
                json.loads(fixed)
                entries.append((key, fixed))
            except Exception:
                bad_count += 1
    if not entries:
        print("No valid entries found. Aborting.")
        return
    outpath = path if overwrite else path + ".pretty.json"
    process_and_pretty_write(outpath, entries, indent)
    print("Formatted", len(entries), "entries written to", outpath)
    if bad_count:
        print("Skipped", bad_count, "invalid entries (file may be truncated).")

if __name__ == "__main__":
    main()
