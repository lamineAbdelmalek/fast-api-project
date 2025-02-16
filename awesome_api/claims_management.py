import hashlib
from typing import List

from pandas import DataFrame

from awesome_api.models import ClaimInfo, ClaimSize, ClaimStatus


def hash_claim_id(claim_id: str) -> str:
    return str(hashlib.sha256(claim_id.encode()).hexdigest())


def set_claim_status(
    initial_claim_amount: int, current_claim_amount: int
) -> ClaimStatus:
    if current_claim_amount == 0:
        return ClaimStatus.SETTLED

    elif current_claim_amount <= (initial_claim_amount / 2):
        return ClaimStatus.PARTIALLY_SETTLED

    else:
        return ClaimStatus.NOT_SETTLED


def set_claim_size(initial_claim_amount: int) -> ClaimSize:
    if initial_claim_amount < 20000:
        return ClaimSize.XS
    elif (initial_claim_amount < 30000) and (initial_claim_amount >= 20000):
        return ClaimSize.S
    elif (initial_claim_amount < 100000) and (initial_claim_amount >= 30000):
        return ClaimSize.M
    elif (initial_claim_amount < 500000) and (initial_claim_amount >= 100000):
        return ClaimSize.L
    elif initial_claim_amount >= 500000:
        return ClaimSize.XL


def get_claim_info(df: DataFrame) -> List[ClaimInfo]:
    claims = []
    for i in range(len(df)):
        row = df.iloc[i]
        claim_info = ClaimInfo(
            claim_creation_date=row["claim_creation_date"].strftime("%Y-%m"),
            company_id=row["debtor_id"],
            hashed_claim_id=hash_claim_id(row["claim_id"]),
            claim_size=set_claim_size(row["initial_claim_amount"]),
            claim_status=set_claim_status(
                row["initial_claim_amount"], row["current_claim_amount"]
            ),
            claim_status_date=row["last_update_date"].strftime("%Y-%m"),
        )
        claims.append(claim_info)

    return claims


def get_claim_info_cp(df: DataFrame) -> List[ClaimInfo]:
    cp = df.copy()
    preprocess_cp(cp)
    record_list = cp.to_dict("records")
    claims = [ClaimInfo.model_validate(record) for record in record_list]
    del cp, record_list
    return claims


def preprocess_cp(cp):
    cp["claim_creation_date"] = cp["claim_creation_date"].dt.strftime("%Y-%m")
    cp["company_id"] = cp["debtor_id"]
    cp["hashed_claim_id"] = cp["claim_id"].apply(hash_claim_id)
    cp["claim_size"] = cp["initial_claim_amount"].apply(set_claim_size)
    cp["claim_status"] = ClaimStatus.NOT_SETTLED
    cp.loc[
        cp["current_claim_amount"] < (cp["initial_claim_amount"] / 2), "claim_status"
    ] = ClaimStatus.PARTIALLY_SETTLED
    cp.loc[cp["current_claim_amount"] == 0, "claim_status"] = ClaimStatus.SETTLED
    cp["claim_status_date"] = cp["last_update_date"].dt.strftime("%Y-%m-%d")
