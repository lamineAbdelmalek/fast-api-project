from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pandas import DataFrame
from pydantic import BaseModel


class DummyScoreModel(BaseModel):
    score_date: str
    score: int
    company_id: str


class DummyClientPortfolioModel(BaseModel):
    company_id: str
    validity_start_date: str


class MonitoringStatus(BaseModel):
    company_id: str
    monitored: bool


class TransactionalQuery(BaseModel):
    query: str
    params: Optional[Dict[str, Any]] = None


class OrderType(Enum):
    SCORES = "scores"
    CLAIMS = "claims"
    SCORE_UPDATES = "score_updates"
    CLAIM_UPDATES = "claim_updates"


class ClientOrder(BaseModel):
    company_id: str
    order_type: OrderType
    order_date: datetime


class SqlRequestExecutor(ABC):
    """Abstract class for data source connection and querying."""

    @abstractmethod
    def run_select_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """Run a SELECT query and return the results."""

    @abstractmethod
    def run_insert_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> None:
        """"""

    @abstractmethod
    def run_update_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> None:
        """"""

    @abstractmethod
    def run_queries_in_one_transaction(self, queries: List[TransactionalQuery]) -> None:
        """"""


class PortfolioManager(ABC):
    @abstractmethod
    def add_company(
        self,
        company_id: str,
        insertion_date: datetime,
        order_type: Optional[OrderType] = None,
    ) -> None:
        """"""

    @abstractmethod
    def remove_company(self, company_id: str, removal_date: datetime) -> None:
        """"""

    @abstractmethod
    def get_portfolio(
        self, only_active_companies: bool = True
    ) -> List[DummyClientPortfolioModel]:
        """"""

    @abstractmethod
    def add_orders(self, orders: List[ClientOrder]):
        pass
