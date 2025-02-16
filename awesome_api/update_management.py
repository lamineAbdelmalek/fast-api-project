from datetime import datetime, timedelta
from typing import List

from awesome_api.claims_management import get_claim_info_cp
from awesome_api.errors import WrongDateFormat
from awesome_api.models import ClaimInfo, ScoreModel
from awesome_api.utils.postgres_utils import PostgresDataSource


def get_score_update(company_id: str, update_date: str) -> List[ScoreModel]:
    now = datetime.now()
    cutoff_date = now - timedelta(days=5 * 365)
    try:
        update_date = datetime.strptime(update_date, "%Y-%m-%d")
    except ValueError:
        raise WrongDateFormat(
            date=update_date,
            message="Wrong date format, enter date in format YYYY-MM-DD",
        )
    next_day = update_date + timedelta(days=1)
    params = {
        "company_id": company_id,
        "cutoff_date": cutoff_date,
        "update_date": update_date,
        "next_day": next_day,
    }
    db = PostgresDataSource()
    df = db.run_select_query(
        query="SELECT score_date, score, company_id FROM company_credit_scores"
        " WHERE company_id = :company_id"
        " and score_date >= :cutoff_date"
        " and score_date >= :update_date"
        " and score_date < :next_day",
        params=params,
    )
    df["score_date"] = df["score_date"].apply(lambda x: x.isoformat())
    if len(df) > 0:
        record_list = df.to_dict("records")
        return [ScoreModel.model_validate(record) for record in record_list]
    else:
        return []


def get_claim_update(company_id: str, update_date: str) -> List[ClaimInfo]:
    now = datetime.now()
    cutoff_date = now - timedelta(days=5 * 365)
    try:
        update_date = datetime.strptime(update_date, "%Y-%m-%d")
    except ValueError:
        raise WrongDateFormat(
            date=update_date,
            message="Wrong date format, enter date in format YYYY-MM-DD",
        )
    next_day = update_date + timedelta(days=1)
    params = {
        "company_id": company_id,
        "cutoff_date": cutoff_date,
        "update_date": update_date,
        "next_day": next_day,
    }

    db = PostgresDataSource()
    df = db.run_select_query(
        query="SELECT claim_creation_date, debtor_id, claim_id, initial_claim_amount, current_claim_amount, last_update_date"
        " FROM claims"
        " WHERE debtor_id = :company_id"
        " and claim_creation_date >= :cutoff_date"
        " and last_update_date >= :update_date"
        " and last_update_date < :next_day",
        params=params,
    )
    if len(df) > 0:
        return get_claim_info_cp(df)
    else:
        return []
