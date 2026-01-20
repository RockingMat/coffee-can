import zipfile
import json
import pandas as pd
import re
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial

# Constants
ZIP_PATH = "../submissions.zip"
OUTPUT_FILE = "submissions_10k.parquet"
BATCH_SIZE = 1000  # Number of files to process per batch in each worker

def process_batch(file_batch):
    """Worker function to process a batch of files from the zip."""
    batch_rows = []
    # Re-open the ZIP in each worker process for thread/process safety
    with zipfile.ZipFile(ZIP_PATH, 'r') as z:
        for name in file_batch:
            if not name.endswith(".json"):
                continue

            # Parse CIK from filename
            cik_from_filename = None
            match = re.search(r'CIK(\d+)', name, re.IGNORECASE)
            if match:
                cik_from_filename = str(int(match.group(1)))

            try:
                with z.open(name) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, Exception):
                continue

            # Identify structure
            if "filings" in data:
                cik = data.get("cik", cik_from_filename)
                filings = data.get("filings", {}).get("recent", {})
            elif "accessionNumber" in data:
                cik = cik_from_filename
                filings = data
            else:
                continue

            if cik:
                cik = str(cik).zfill(10)

            forms = filings.get("form", [])
            dates = filings.get("filingDate", [])
            accessions = filings.get("accessionNumber", [])
            primary_docs = filings.get("primaryDocument", [])

            for form, date, acc, doc in zip(forms, dates, accessions, primary_docs):
                if form == "10-K" and doc and doc.strip():
                    batch_rows.append({
                        "cik": cik,
                        "filing_date": date,
                        "accession": acc,
                        "primary_document": doc
                    })
    return batch_rows

def main():
    print(f"Opening {ZIP_PATH}...")
    try:
        with zipfile.ZipFile(ZIP_PATH, 'r') as z:
            all_files = [n for n in z.namelist() if n.endswith(".json")]
            total_files = len(all_files)
            print(f"Total JSON files to process: {total_files}")

        # Split files into batches
        batches = [all_files[i:i + BATCH_SIZE] for i in range(0, total_files, BATCH_SIZE)]
        print(f"Split into {len(batches)} batches of {BATCH_SIZE} files.")

        rows = []
        # Use CPU count to decide number of processes
        max_workers = os.cpu_count() or 4
        print(f"Starting extraction using {max_workers} processes...")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}
            
            completed = 0
            for future in as_completed(future_to_batch):
                result = future.result()
                rows.extend(result)
                completed += 1
                if completed % 10 == 0 or completed == len(batches):
                    print(f"Progress: {completed}/{len(batches)} batches done (Filings collected: {len(rows)})")

        print("Creating DataFrame...")
        df = pd.DataFrame(rows)
        print(f"Saving to {OUTPUT_FILE}...")
        df.to_parquet(OUTPUT_FILE)
        print(f"Success! Processed {len(rows)} total 10-K filings.")

    except FileNotFoundError:
        print(f"Error: {ZIP_PATH} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
