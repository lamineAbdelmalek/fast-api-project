from typing import Any, Dict


class AwesomeApiError(Exception):
    def __init__(self, **metadata):
        self.metadata: Dict[str, Any] = metadata


class MultipleMonitoringError(AwesomeApiError):
    def __init__(self, company_id: str, **other_metadata):
        self.company_id: str = company_id
        metadata = {"company_id": company_id}
        metadata.update(other_metadata)
        super().__init__(**metadata)


class WrongDateFormat(AwesomeApiError):
    def __init__(self, date: str, **other_metadata):
        self.date: str = date
        metadata = {"date": date}
        metadata.update(other_metadata)
        super().__init__(**metadata)
