import os
from typing import Any, Dict, List, Optional

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
from awesome_api.models import SqlRequestExecutor, TransactionalQuery


class PostgresDataSource(SqlRequestExecutor):
    """PostgreSQL implementation of BaseDataSource."""

    def __init__(self):
        """Class init"""

    def run_select_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        # Get database url
        engine = self._create_db_engine()

        # Execute the query and load results into a Pandas DataFrame
        if params:
            df = pd.read_sql(text(query), engine, params=params)
        else:
            df = pd.read_sql(text(query), engine)
        # Close the connection
        engine.dispose()

        return df

    def run_insert_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> None:
        engine = self._create_db_engine()
        with engine.connect() as connection:
            if params:
                connection.execute(text(query), params)
            else:
                connection.execute(text(query))
        engine.dispose()

    def run_update_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> None:
        engine = self._create_db_engine()
        with engine.connect() as connection:
            if params:
                connection.execute(text(query), params)
            else:
                connection.execute(text(query))
        engine.dispose()

    def _create_db_engine(self):
        database_url = self.get_db_url()
        # Create a database engine
        engine = create_engine(database_url)
        return engine

    def run_queries_in_one_transaction(self, queries: List[TransactionalQuery]) -> None:
        engine = self._create_db_engine()
        with engine.connect() as connection:
            transaction = connection.begin()  # Start the transaction
            try:
                # Execute multiple queries
                for query in queries:
                    connection.execute(text(query.query), query.params)
                # Commit the transaction if all queries succeed
                transaction.commit()
            except Exception as e:
                # Roll back the transaction in case of any error
                transaction.rollback()
                print(f"Transaction rolled back due to: {e}")
                raise e

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
