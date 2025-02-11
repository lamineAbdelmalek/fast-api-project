from datetime import datetime, timedelta
from typing import List

from fastapi import FastAPI

from awesome_api.claims_management import (
    hash_claim_id,
    set_claim_size,
    set_claim_status,
)
from awesome_api.models import (
    ClaimInfo,
    DummyClientPortfolioModel,
    DummyScoreModel,
    MonitoringStatus,
    OrderType,
)
from awesome_api.portfolio_management import SqlPortfolioManager
from awesome_api.utils.postgres_utils import PostgresDataSource

app = FastAPI()


@app.get("/dummy", response_model=List[DummyScoreModel])
def get_dummy_two_scores_records():
    db = PostgresDataSource()
    df = db.run_select_query(query="SELECT * FROM company_credit_scores LIMIT 2;")
    del df["score_type"]
    df["score_date"] = df["score_date"].apply(lambda x: x.isoformat())
    records = df.to_dict("records")
    return [DummyScoreModel.model_validate(record) for record in records]


@app.get("/{company_id}/scores", response_model=List[DummyScoreModel])
def get_scores(company_id: str):
    now = datetime.now()
    cutoff_date = now - timedelta(days=5 * 365)
    params = {"company_id": company_id, "cutoff_date": cutoff_date}
    db = PostgresDataSource()
    df = db.run_select_query(
        query="SELECT score_date, score, company_id FROM company_credit_scores"
        " WHERE company_id = :company_id"
        " and score_date >= :cutoff_date;",
        params=params,
    )
    df["score_date"] = df["score_date"].apply(lambda x: x.isoformat())
    records = df.to_dict("records")
    pf_manager = SqlPortfolioManager(executor=db)
    pf_manager.add_company(
        company_id=company_id, insertion_date=now, order_type=OrderType.SCORES
    )
    return [DummyScoreModel.model_validate(record) for record in records]


@app.get("/{company_id}/claims", response_model=list[ClaimInfo])
def get_claims(company_id: str):
    now = datetime.now()
    params = {"company_id": company_id}
    db = PostgresDataSource()
    df = db.run_select_query(
        query="SELECT claim_creation_date, debtor_id, claim_id, initial_claim_amount, current_claim_amount, last_update_date"
        " FROM claims"
        " WHERE debtor_id = :company_id",
        params=params,
    )
    claims = []
    for i in range(len(df)):
        claim_info = {
            "claim_creation_date": df.iloc[i]["claim_creation_date"].strftime("%Y-%m"),
            "company_id": df.iloc[i]["debtor_id"],
            "hashed_claim_id": hash_claim_id(df.iloc[i]["claim_id"]),
            "claim_size": set_claim_size(df.iloc[i]["initial_claim_amount"]),
            "claim_status": set_claim_status(
                df.iloc[i]["initial_claim_amount"], df.iloc[i]["current_claim_amount"]
            ),
            "claim_status_date": df.iloc[i]["last_update_date"].strftime("%Y-%m"),
        }
        claims.append(claim_info)
    pf_manager = SqlPortfolioManager(executor=db)
    pf_manager.add_company(
        company_id=company_id, insertion_date=now, order_type=OrderType.CLAIMS
    )
    return [ClaimInfo.model_validate(claim) for claim in claims]


@app.get("/client_portfolio", response_model=List[DummyClientPortfolioModel])
def get_portfolio():
    db = PostgresDataSource()
    pf_manager = SqlPortfolioManager(executor=db)
    return pf_manager.get_portfolio()


@app.delete("/delete_company/{company_id}", response_model=MonitoringStatus)
def delete_company(company_id: str):
    db = PostgresDataSource()
    pf_manager = SqlPortfolioManager(executor=db)
    pf_manager.remove_company(company_id=company_id, removal_date=datetime.now())
    return MonitoringStatus(company_id=company_id, monitored=False)


@app.get("/hello")
def hello_world():
    return "Hello World"
