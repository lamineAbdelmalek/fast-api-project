from typing import List

from fastapi import FastAPI
from pydantic import BaseModel

from awesome_api.utils.postgres_utils import run_select_query

app = FastAPI()


class DummyScoreModel(BaseModel):
    score_date: str
    score: int
    company_id: str


@app.get("/dummy", response_model=List[DummyScoreModel])
def get_dummy_two_scores_records():
    df = run_select_query(query="SELECT * FROM company_credit_scores LIMIT 2;")
    del df["score_type"]
    df["score_date"] = df["score_date"].apply(lambda x: x.isoformat())
    records = df.to_dict("records")
    return [DummyScoreModel.model_validate(record) for record in records]


@app.get("/hello")
def hello_world():
    return "Hello World"
