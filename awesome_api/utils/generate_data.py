import csv
import random
import string
from datetime import datetime, timedelta

# Configuration
NUM_COMPANIES = 50
MAX_HISTORY = 100  # Maximum records per company
OUTPUT_FILE = "initial_data/company_credit_scores.csv"


# Function to generate a random company ID
def generate_company_id() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=12))


# Generate a fixed set of company IDs
COMPANY_IDS = [generate_company_id() for _ in range(NUM_COMPANIES)]


# Function to generate random score records
def generate_score_history(company_id: str):
    num_records = random.randint(
        10, MAX_HISTORY
    )  # Random number of history records per company
    base_date = datetime.now()

    records = []
    for _ in range(num_records):
        score_date = base_date - timedelta(
            days=random.randint(0, 3650)
        )  # Random date within last 10 years
        score = random.randint(0, 5)
        score_type = random.choice(["X", "Y"])
        records.append(
            [score_date.strftime("%Y-%m-%d %H:%M:%S"), company_id, score, score_type]
        )

    return records


def main():
    # Generate data
    all_records = []
    for company_id in COMPANY_IDS:
        all_records.extend(generate_score_history(company_id))

    # Write to CSV
    with open(OUTPUT_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["SCORE_DATE", "COMPANY_ID", "SCORE", "SCORE_TYPE"])
        writer.writerows(all_records)

    print(f"Generated {len(all_records)} records and saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
