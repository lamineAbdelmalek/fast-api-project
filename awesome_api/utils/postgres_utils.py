import os

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import create_engine

from awesome_api.constants import (
    ENV_VAR_POSTGRES_DATABASE_URL,
    ENV_VAR_POSTGRES_DB,
    ENV_VAR_POSTGRES_PASSWORD,
    ENV_VAR_POSTGRES_USER,
)


def run_select_query(query: str) -> DataFrame:
    # Get database url
    database_url = get_db_url()

    # Create a database engine
    engine = create_engine(database_url)

    # Execute the query and load results into a Pandas DataFrame
    df = pd.read_sql(query, engine)

    # Close the connection
    engine.dispose()

    return df


def get_db_url() -> str:
    if ENV_VAR_POSTGRES_DATABASE_URL in os.environ:
        # docker mode
        db_url = os.environ[ENV_VAR_POSTGRES_DATABASE_URL]
    else:
        # local mode
        load_dotenv(".env")
        user = os.environ[ENV_VAR_POSTGRES_USER]
        password = os.environ[ENV_VAR_POSTGRES_PASSWORD]
        db_name = os.environ[ENV_VAR_POSTGRES_DB]
        host = "localhost"
        db_url = f"postgresql://{user}:{password}@{host}:5432/{db_name}"
    return db_url


def run_sql_example() -> None:
    df = run_select_query(query="SELECT * FROM company_credit_scores LIMIT 10;")
    print(df)
