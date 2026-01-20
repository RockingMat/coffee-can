import zipfile
import json
import pandas as pd

rows = []

with zipfile.ZipFile("../submissions.zip") as z:
    for name in z.namelist():
        if not name.endswith(".json"):
            continue

        with z.open(name) as f:
            data = json.load(f)

        filings = data.get("filings", {}).get("recent", {})
        forms = filings.get("form", [])
        dates = filings.get("filingDate", [])
        accessions = filings.get("accessionNumber", [])

        for form, date, acc in zip(forms, dates, accessions):
            if form == "10-K":
                rows.append({
                    "cik": data["cik"],
                    "filing_date": date,
                    "accession": acc
                })

df = pd.DataFrame(rows)
