import zipfile
import json
import pandas as pd
import re
import os

rows = []

print("Processing submissions.zip...")

try:
    with zipfile.ZipFile("../submissions.zip") as z:
        all_files = z.namelist()
        print(f"Total files available in zip: {len(all_files)}")
        
        processed_count = 0
        LIMIT = 10  # Limit for testing purposes

        for name in all_files:
            if LIMIT is not None and processed_count >= LIMIT:
                print(f"Hit limit of {LIMIT} files. Stopping loop.")
                break

            if not name.endswith(".json"):
                continue
            
            processed_count += 1

            # Parse CIK from filename (format: CIKxxxxxxxxxx.json or ...-submissions-xxx.json)
            # This is reliable even if the JSON relies on context
            cik_from_filename = None
            match = re.search(r'CIK(\d+)', name, re.IGNORECASE)
            if match:
                cik_from_filename = str(int(match.group(1)))

            with z.open(name) as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    continue

            # Initialize variables
            cik = None
            ticker = None
            title = ""
            filings = {}

            # Determine file structure (Main vs Extension)
            if "filings" in data:
                # Main submission file
                cik = data.get("cik", cik_from_filename)
                ticker_list = data.get("tickers", [])
                ticker = ticker_list[0] if ticker_list else None
                title = data.get("name", "")
                filings = data.get("filings", {}).get("recent", {})

                # DEBUG: Check keys
                if len(rows) < 5:
                    print(f"\n[DEBUG] {name} (Main): Ticker={ticker}, Name={title}")
                    if "primaryDocument" not in filings:
                        print(f"    WARNING: 'primaryDocument' NOT found. Available keys: {list(filings.keys())}")
                    else:
                        print(f"    'primaryDocument' found (Length: {len(filings['primaryDocument'])})")

            elif "accessionNumber" in data:
                # Extension file (e.g. CIK...-submissions-001.json)
                cik = cik_from_filename
                # Extension files typically lack metadata like ticker/name
                filings = data

                # DEBUG: Check keys
                if len(rows) < 5:
                    print(f"\n[DEBUG] {name} (Extension): CIK={cik}")
                    if "primaryDocument" not in filings:
                        print(f"    WARNING: 'primaryDocument' NOT found. Available keys: {list(filings.keys())}")
                    else:
                        print(f"    'primaryDocument' found (Length: {len(filings['primaryDocument'])})")
            else:
                # Unknown format, skip
                print(f"[DEBUG] {name}: Unknown format. Keys: {list(data.keys())}")
                continue

            # Ensure CIK is exactly 10 digits
            if cik:
                cik = str(cik).zfill(10)

            forms = filings.get("form", [])
            dates = filings.get("filingDate", [])
            accessions = filings.get("accessionNumber", [])
            primary_docs = filings.get("primaryDocument", [])

            # Ensure all lists are zipped
            for form, date, acc, doc in zip(forms, dates, accessions, primary_docs):
                # Filter for 10-K AND ensure primary_document is not empty
                if form == "10-K" and doc and doc.strip():
                    rows.append({
                        "cik": cik,
                        "filing_date": date,
                        "accession": acc,
                        "primary_document": doc
                    })

    df = pd.DataFrame(rows)
    output_file = "submissions_10k.parquet"
    df.to_parquet(output_file)
    
    # Simple summary stats instead of stats check loop
    print(f"Successfully processed {len(rows)} filings and saved to {output_file}")
    
    if not df.empty:
        print("\nSample of generated data:")
        print(df.head())
    else:
        print("\nWarning: DataFrame is empty. No 10-K filings with primary documents found.")

except FileNotFoundError:
    print("Error: ../submissions.zip not found. Please ensure the file exists.")
except Exception as e:
    print(f"An error occurred: {e}")
