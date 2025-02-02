import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from awesome_api.constants import (
    ENV_VAR_POSTGRES_DATABASE_URL,
    ENV_VAR_POSTGRES_DB,
    ENV_VAR_POSTGRES_PASSWORD,
    ENV_VAR_POSTGRES_USER,
)


class BaseDataSource(ABC):
    """Abstract class for data source connection and querying."""

    @abstractmethod
    def get_db_url(self) -> str:
        """Establish a connection to the data source."""
        pass

    @abstractmethod
    def run_select_query(self, query: str) -> DataFrame:
        """Run a SELECT query and return the results."""
        pass


class PostgresDataSource(BaseDataSource):
    """PostgreSQL implementation of BaseDataSource."""

    def __init__(self):
        """Class init"""

    def run_select_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        # Get database url
        database_url = self.get_db_url()

        # Create a database engine
        engine = create_engine(database_url)

        # Execute the query and load results into a Pandas DataFrame
        if params:
            df = pd.read_sql(text(query), engine, params=params)
        else:
            df = pd.read_sql(text(query), engine)
        # Close the connection
        engine.dispose()

        return df

    def get_db_url(self) -> str:
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

    def run_sql_example(self) -> None:
        df = self.run_select_query(
            query="SELECT * FROM company_credit_scores LIMIT 10;"
        )
        print(df)
