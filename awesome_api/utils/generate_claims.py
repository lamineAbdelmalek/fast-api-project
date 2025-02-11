import csv
import random
import string
from datetime import datetime, timedelta
from typing import List

import pandas as pd
from pandas import DataFrame

# Configuration
NUM_CLAIMS = 500
MAX_HISTORY = 10  # Maximum records per company
MAX_CLAIM_PER_COMPANY = 5
SCORE_FILE = "initial_data/company_credit_scores.csv"
OUTPUT_FILE = "initial_data/company_claims.csv"
SAMPLE_SIZE = 3
MIN_CLAIM_AMOUNT = 10000
MAX_CLAIM_AMOUNT = 1000000


def generate_random_dates(number_of_dates: int, years: int) -> List[datetime]:
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years * 365)

    random_dates = [
        start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        for _ in range(number_of_dates)
    ]
    return random_dates


def get_score_by_date(df: DataFrame, company_id: str, date: datetime) -> int:
    comp_scores = df.loc[(df["COMPANY_ID"] == company_id)]
    score_after_date = comp_scores.loc[df["SCORE_DATE"] > date]
    if len(score_after_date) > 0:
        return score_after_date.sort_values(["SCORE_DATE"]).iloc[0]["SCORE"]
    else:
        return comp_scores.sort_values(["SCORE_DATE"], ascending=False).iloc[0]["SCORE"]


def flag_claim(last_active_score: int) -> bool:
    y = random.random()
    if y <= 1 / (last_active_score + 2):
        return True
    else:
        return False


def generate_update_date(creation_date: datetime) -> datetime:
    end_date = datetime.now()
    was_it_updated = random.random()
    if was_it_updated > 0.5:
        return creation_date + timedelta(
            days=random.randint(0, (end_date - creation_date).days)
        )
    else:
        return creation_date


def generate_claim_id() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=20))


def generate_random_id() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=12))


def set_current_claim_amount(
    initial_amount: int, claim_creation_date: datetime, claim_update_date: datetime
) -> int:
    if claim_update_date > claim_creation_date:
        return initial_amount - random.randint(100, initial_amount)
    else:
        return initial_amount


def generate_claim(df: DataFrame):
    datetime_claims = generate_random_dates(
        number_of_dates=NUM_CLAIMS, years=MAX_HISTORY
    )
    records = []
    unique_ids = list(df["COMPANY_ID"].unique())
    for i in range(len(datetime_claims)):
        sampled_ids = random.sample(unique_ids, SAMPLE_SIZE)
        for j in range(len(sampled_ids)):
            score = get_score_by_date(
                df=df, company_id=sampled_ids[j], date=datetime_claims[i]
            )
            if flag_claim(score):
                claim_id = generate_claim_id()
                claim_creation_date = datetime_claims[i]
                debtor_id = sampled_ids[j]
                client_id = generate_random_id()
                last_update_date = generate_update_date(
                    creation_date=datetime_claims[i]
                )
                initial_claim_amount = random.randint(
                    MIN_CLAIM_AMOUNT, MAX_CLAIM_AMOUNT
                )
                current_claim_amount = set_current_claim_amount(
                    initial_amount=initial_claim_amount,
                    claim_creation_date=claim_creation_date,
                    claim_update_date=last_update_date,
                )
                records.append(
                    [
                        claim_id,
                        claim_creation_date.strftime("%Y-%m-%d %H:%M:%S"),
                        debtor_id,
                        client_id,
                        last_update_date.strftime("%Y-%m-%d %H:%M:%S"),
                        initial_claim_amount,
                        current_claim_amount,
                    ]
                )
    return records


def main():
    # Generate data
    df = pd.read_csv(SCORE_FILE)
    df["SCORE_DATE"] = pd.to_datetime(df["SCORE_DATE"])
    all_claims = generate_claim(df=df)
    # Write to CSV
    with open(OUTPUT_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "CLAIM_ID",
                "CLAIM_CREATION_DATE",
                "DEBTOR_ID",
                "CLIENT_ID",
                "LAST_UPDATE_DATE",
                "INITIAL_CLAIM_AMOUNT",
                "CURRENT_CLAIM_AMOUNT",
            ]
        )
        writer.writerows(all_claims)

    print(f"Generated {len(all_claims)} records and saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
