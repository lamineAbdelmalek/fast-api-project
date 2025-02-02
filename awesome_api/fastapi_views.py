from datetime import datetime, timedelta
from typing import List

from fastapi import FastAPI

from awesome_api.models import DummyClientPortfolioModel, DummyScoreModel, OrderType
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


@app.get("/client_portfolio", response_model=List[DummyClientPortfolioModel])
def get_portfolio():
    db = PostgresDataSource()
    pf_manager = SqlPortfolioManager(executor=db)
    return pf_manager.get_portfolio()


@app.get("/hello")
def hello_world():
    return "Hello World"
