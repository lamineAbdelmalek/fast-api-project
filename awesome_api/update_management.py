from datetime import datetime, timedelta
from functools import partial
from typing import List, Sequence

from pandas import DataFrame

from awesome_api.claims_management import get_claim_info_cp
from awesome_api.models import ClaimInfo, ScoreModel
from awesome_api.utils.parallel_extraction import parallel_execution
from awesome_api.utils.postgres_utils import PostgresDataSource
from awesome_api.utils.sql_utils import parametrized_in_clause, generate_param_dict


def get_score_update(
    company_id: str, update_date: datetime, call_date: datetime
) -> List[ScoreModel]:
    cutoff_date = call_date - timedelta(days=5 * 365)
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
    record_list = df.to_dict("records")
    return [ScoreModel.model_validate(record) for record in record_list]


def get_claim_update(
    company_id: str, update_date: datetime, call_date: datetime
) -> List[ClaimInfo]:
    cutoff_date = call_date - timedelta(days=5 * 365)
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
    return get_claim_info_cp(df)

def get_score_updates_companies_chunk(
    chunk: Sequence[str], update_date: datetime, call_date: datetime
) -> DataFrame:
    if len(chunk)==0:
        raise ValueError("Chunk must not be empty")
    if len(chunk) > 1000:
        raise ValueError("Chunk size must not exceed 1000")
    cutoff_date = call_date - timedelta(days=5 * 365)
    next_day = update_date + timedelta(days=1)
    db = PostgresDataSource()
    prefix = "company"
    in_clause = parametrized_in_clause(size=len(chunk), prefix=prefix)
    company_param_dict = generate_param_dict(values=chunk, prefix=prefix)
    params = {
        "cutoff_date": cutoff_date,
        "update_date": update_date,
        "next_day": next_day,
        **company_param_dict
    }
    df = db.run_select_query(
        query="SELECT score_date, score, company_id FROM company_credit_scores"
        f" WHERE company_id {in_clause}"
        " and score_date >= :cutoff_date"
        " and score_date >= :update_date"
        " and score_date < :next_day",
        params=params,
    )
    df["score_date"] = df["score_date"].apply(lambda x: x.isoformat())

    # return [ScoreModel.model_validate(record) for record in record_list]
    return df

def get_score_updates_companies(
    companies:Sequence[str],
    update_date: datetime,
    call_date: datetime,
    chunk_size: int,
    pool_size:int,
) -> List[ScoreModel]:
    extraction_func = partial(
        get_score_updates_companies_chunk,
        update_date=update_date,
        call_date=call_date,
    )
    res = parallel_execution(func=extraction_func, values=companies, chunk_size=chunk_size, pool_size=pool_size)
    return [ScoreModel.model_validate(record) for record in res.to_dict("records")]

if __name__ == '__main__':
    example = get_score_updates_companies_chunk(
        chunk=['NVH0D651WH34', '5U0YGGPQNRT6', 'H5BG77SAYJN0', 'XOLEJ1U4XNHU'],
        update_date = datetime(2024, 4, 14),
        call_date=datetime.now(),
    )
    example2 = get_score_updates_companies(
        companies=['NVH0D651WH34', '5U0YGGPQNRT6', 'H5BG77SAYJN0', 'XOLEJ1U4XNHU'],
        update_date=datetime(2024, 4, 14),
        call_date=datetime.now(),
        chunk_size=2,
        pool_size=4
    )
    print(example.shape)
    print(example2)