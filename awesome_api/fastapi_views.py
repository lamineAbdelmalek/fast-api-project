from datetime import datetime, timedelta
from typing import List

from fastapi import FastAPI

from awesome_api.claims_management import get_claim_info_cp
from awesome_api.models import (
    ClaimInfo,
    ClientPortfolioModel,
    ClientUpdate,
    MonitoringStatus,
    OrderType,
    ScoreModel,
)
from awesome_api.portfolio_management import SqlPortfolioManager
from awesome_api.update_management import get_claim_update, get_score_update
from awesome_api.utils.postgres_utils import PostgresDataSource

app = FastAPI()


@app.get("/dummy", response_model=List[ScoreModel])
def get_dummy_two_scores_records():
    db = PostgresDataSource()
    df = db.run_select_query(query="SELECT * FROM company_credit_scores LIMIT 2;")
    del df["score_type"]
    df["score_date"] = df["score_date"].apply(lambda x: x.isoformat())
    records = df.to_dict("records")
    return [ScoreModel.model_validate(record) for record in records]


@app.get("/{company_id}/scores", response_model=List[ScoreModel])
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
    return [ScoreModel.model_validate(record) for record in records]


@app.get("/{company_id}/claims", response_model=list[ClaimInfo])
def get_claims(company_id: str):
    now = datetime.now()
    cutoff_date = now - timedelta(days=5 * 365)
    params = {"company_id": company_id, "cutoff_date": cutoff_date}
    db = PostgresDataSource()
    df = db.run_select_query(
        query="SELECT claim_creation_date, debtor_id, claim_id, initial_claim_amount, current_claim_amount, last_update_date"
        " FROM claims"
        " WHERE debtor_id = :company_id"
        " and claim_creation_date >= :cutoff_date;",
        params=params,
    )
    claims = get_claim_info_cp(df)
    pf_manager = SqlPortfolioManager(executor=db)
    pf_manager.add_company(
        company_id=company_id, insertion_date=now, order_type=OrderType.CLAIMS
    )
    return claims


@app.get("/{update_date}/updates", response_model=ClientUpdate)
def get_updates(update_date: str):
    db = PostgresDataSource()
    pf_manager = SqlPortfolioManager(executor=db)
    now = datetime.now()
    monitored_companies = pf_manager.get_portfolio()
    score_updates = []
    claim_updates = []
    for company in monitored_companies:
        score_updates.extend(
            get_score_update(company_id=company.company_id, update_date=update_date)
        )
        pf_manager.add_company(
            company_id=company.company_id,
            insertion_date=now,
            order_type=OrderType.SCORE_UPDATES,
        )
        claim_updates.extend(
            get_claim_update(company_id=company.company_id, update_date=update_date)
        )
        pf_manager.add_company(
            company_id=company.company_id,
            insertion_date=now,
            order_type=OrderType.CLAIM_UPDATES,
        )
    return ClientUpdate(
        update_date=update_date,
        score_updates=score_updates,
        claim_updates=claim_updates,
    )


@app.get("/client_portfolio", response_model=List[ClientPortfolioModel])
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
