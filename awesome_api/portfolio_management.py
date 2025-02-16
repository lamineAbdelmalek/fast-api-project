from datetime import datetime
from typing import Any, List, Optional

import pandas as pd
from pandas import DataFrame

from awesome_api.errors import MultipleMonitoringError
from awesome_api.models import (
    ClientOrder,
    ClientPortfolioModel,
    OrderType,
    PortfolioManager,
    TransactionalQuery,
)
from awesome_api.utils.postgres_utils import SqlRequestExecutor


class SqlPortfolioManager(PortfolioManager):

    def __init__(self, executor: SqlRequestExecutor):
        self.executor: SqlRequestExecutor = executor

    @staticmethod
    def _build_insert_company_query(
        company_id: str, insertion_date: datetime
    ) -> TransactionalQuery:
        query = """
        INSERT INTO client_portfolio (company_id, validity_start_date, is_valid)
         Values (:company_id, :insertion_date, :is_valid);
        """
        params = {
            "company_id": company_id,
            "insertion_date": insertion_date,
            "is_valid": 1,
        }
        return TransactionalQuery(query=query, params=params)

    @staticmethod
    def _build_insert_order_query(order: ClientOrder) -> TransactionalQuery:
        query = """
        INSERT INTO client_orders (COMPANY_ID, ORDER_DATE, ORDER_TYPE)
         Values (:company_id, :order_date, :order_type);
        """
        params = {
            "company_id": order.company_id,
            "order_date": order.order_date,
            "order_type": order.order_type.value,
        }
        return TransactionalQuery(query=query, params=params)

    @staticmethod
    def _build_stop_monitoring_query(
        end_date: datetime, portfolio_entry_id: Any
    ) -> TransactionalQuery:
        query = """
        UPDATE client_portfolio
        SET
            validity_end_date = :end_date
        WHERE
            portfolio_entry_id = :portfolio_entry_id;
        """
        params = {
            "end_date": end_date,
            "portfolio_entry_id": portfolio_entry_id,
        }
        return TransactionalQuery(query=query, params=params)

    @staticmethod
    def _build_change_last_valid_monitoring_query(
        portfolio_entry_id: Any,
    ) -> TransactionalQuery:
        query = """
        UPDATE client_portfolio
        SET
            IS_VALID = :is_valid
        WHERE
            portfolio_entry_id = :portfolio_entry_id;
        """
        params = {
            "is_valid": 0,
            "portfolio_entry_id": portfolio_entry_id,
        }
        return TransactionalQuery(query=query, params=params)

    def get_company_data(self, company_id: str) -> DataFrame:
        params = {"company_id": company_id}
        query = """
        SELECT * FROM client_portfolio
        WHERE company_id = :company_id
        AND is_valid = 1;
        """
        return self.executor.run_select_query(query=query, params=params)

    def add_company(
        self,
        company_id: str,
        insertion_date: datetime,
        order_type: Optional[OrderType] = None,
    ) -> None:
        df = self.get_company_data(company_id=company_id)

        if len(df) > 1:
            raise MultipleMonitoringError(
                company_id=company_id, number_of_records=len(df)
            )

        if order_type is None:
            queries: List[TransactionalQuery] = []
        else:
            queries = [
                self._build_insert_order_query(
                    order=ClientOrder(
                        company_id=company_id,
                        order_type=order_type,
                        order_date=insertion_date,
                    )
                )
            ]

        if len(df) == 0:
            queries.append(
                self._build_insert_company_query(
                    company_id=company_id, insertion_date=insertion_date
                )
            )
        else:
            if not pd.isnull(df["validity_end_date"].iloc[0]):
                queries += [
                    self._build_change_last_valid_monitoring_query(
                        portfolio_entry_id=df["portfolio_entry_id"].iloc[0]
                    ),
                    self._build_insert_company_query(
                        company_id=company_id, insertion_date=insertion_date
                    ),
                ]

        self.executor.run_queries_in_one_transaction(queries=queries)

    def remove_company(self, company_id: str, removal_date: datetime) -> None:
        df = self.get_company_data(company_id=company_id)
        if len(df) > 1:
            raise MultipleMonitoringError(
                company_id=company_id, number_of_records=len(df)
            )
        elif len(df) == 1:
            if pd.isnull(df["validity_end_date"].iloc[0]):
                self.executor.run_queries_in_one_transaction(
                    queries=[
                        self._build_stop_monitoring_query(
                            end_date=removal_date,
                            portfolio_entry_id=int(df["portfolio_entry_id"].iloc[0]),
                        )
                    ]
                )

    def add_orders(self, orders: List[ClientOrder]):
        queries = [self._build_insert_order_query(order=order) for order in orders]
        self.executor.run_queries_in_one_transaction(queries=queries)

    def get_portfolio(
        self, only_active_companies: bool = True
    ) -> List[ClientPortfolioModel]:
        query = "select * from client_portfolio where validity_end_date is null;"
        df = self.executor.run_select_query(query=query)
        for col in ["validity_start_date"]:
            df[col] = df[col].apply(lambda x: x.isoformat())
        return [
            ClientPortfolioModel.model_validate(record)
            for record in df.to_dict("records")
        ]
