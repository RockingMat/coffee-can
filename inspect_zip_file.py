import zipfile
import sys
import json

# Usage: python3 inspect_zip_file.py CIK0000004926.json

if len(sys.argv) < 2:
    print("Please provide the filename inside the zip to inspect.")
    print("Example: python3 inspect_zip_file.py CIK0000004926.json")
    sys.exit(1)

target_file = sys.argv[1]
zip_path = "../submissions.zip"

try:
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Check if file exists
        if target_file not in z.namelist():
            print(f"Error: '{target_file}' not found in {zip_path}")
            sys.exit(1)

        print(f"--- Extracting {target_file} ---")
        with z.open(target_file) as f:
            # Load json to pretty print it
            try:
                data = json.load(f)
                print(json.dumps(data, indent=2))
            except json.JSONDecodeError:
                # Fallback for non-json or malformed
                f.seek(0)
                print(f.read().decode('utf-8'))

except FileNotFoundError:
    print(f"Error: {zip_path} not found.")
except Exception as e:
    print(f"An error occurred: {e}")
