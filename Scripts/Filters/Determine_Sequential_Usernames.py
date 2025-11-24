import os
import re
import shutil

def main():
    source = input("Path to the JSON collection: ").strip()
    if not os.path.isdir(source):
        print("Invalid path.")
        return

    dest = os.path.join(os.getcwd(), "Sequential Accounts")
    os.makedirs(dest, exist_ok=True)

    pattern = re.compile(r"^base_\d+\.json$")

    for entry in os.listdir(source):
        if pattern.match(entry):
            src_path = os.path.join(source, entry)
            if os.path.isfile(src_path):
                shutil.move(src_path, os.path.join(dest, entry))

    print("Completed.")

if __name__ == "__main__":
    main()