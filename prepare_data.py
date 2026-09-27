import csv
import gzip


# these are the required fields
fields = [
    "EVENT_ID",
    "STATE",
    "YEAR",
    "MONTH_NAME",
    "EVENT_TYPE",
    "CZ_TYPE",
    "CZ_NAME",
    "INJURIES_DIRECT",
    "INJURIES_INDIRECT",
    "DEATHS_DIRECT",
    "DEATHS_INDIRECT",
    "DAMAGE_PROPERTY",
    "DAMAGE_CROPS",
    "TOR_F_SCALE",
]

source = "StormEvents_details-ftp_v1.0_d1950_c20260323.csv.gz"

with gzip.open(source, "rt", encoding="utf-8", newline="") as infile:
    reader = csv.DictReader(infile)

    # checking the header before doing it
    for field in fields:
        if field not in (reader.fieldnames or []):
            raise ValueError(f"Missing required column: {field}")

    with open("details-1950.csv", "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(fields)

        count = 0
        for row in reader:
            record = []
            for field in fields:
                record.append(row[field])

            writer.writerow(record)
            count += 1

print(f"Created details-1950.csv with {count} records and {len(fields)} columns")