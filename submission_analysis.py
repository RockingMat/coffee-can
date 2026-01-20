import zipfile
import json
import pandas as pd

rows = []

print("Processing submissions.zip...")

try:
    with zipfile.ZipFile("../submissions.zip") as z:
        for name in z.namelist():
            if not name.endswith(".json"):
                continue

            with z.open(name) as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    continue
            # I am curious how can I see the fields of the json? See what is being outputted etc.
            # What I really want is these filings yes.
            # I want this to be indexed on tickers so that it is easy to add in stock data.
            cik = data.get("cik")
            
            # DEBUG: Print structure for the first few files to verify keys
            if len(rows) < 5:
                print(f"\n--- Inspecting JSON for {name} ---")
                print(f"Top-level keys: {list(data.keys())}")
                if "tickers" in data:
                    print(f"Tickers found: {data['tickers']}")
                else:
                    print("Tickers key NOT found")
                
                if "name" in data:
                    print(f"Name found: {data['name']}")
                
                filings_debug = data.get("filings", {}).get("recent", {})
                print(f"Filings keys: {list(filings_debug.keys())}")
                if "primaryDocument" not in filings_debug:
                    print("WARNING: primaryDocument not found in filings")
            
            # Submissions JSON usually has 'tickers' as a list
            ticker_list = data.get("tickers", [])
            ticker = ticker_list[0] if ticker_list else None
            title = data.get("name", "")

            filings = data.get("filings", {}).get("recent", {})
            forms = filings.get("form", [])
            dates = filings.get("filingDate", [])
            accessions = filings.get("accessionNumber", [])
            primary_docs = filings.get("primaryDocument", [])

            # Ensure all lists are zipped; usually they align in the SEC packet
            # but using zip ensures we don't go out of bounds if one is shorter
            for form, date, acc, doc in zip(forms, dates, accessions, primary_docs):
                if form == "10-K":
                    rows.append({
                        "cik": cik,
                        "ticker": ticker,
                        "title": title,
                        "filing_date": date,
                        "accession": acc,
                        "primary_document": doc
                    })

    df = pd.DataFrame(rows)
    output_file = "submissions_10k.parquet"
    df.to_parquet(output_file)
    print(f"Successfully processed {len(rows)} filings and saved to {output_file}")

except FileNotFoundError:
    print("Error: ../submissions.zip not found. Please ensure the file exists.")
except Exception as e:
    print(f"An error occurred: {e}")
