from lib.datalib.service import DataService
from lib.datalib.model import SensorDataDTO
from polars import DataFrame


class ServerService():
    def __init__(self):
        self.data_service = DataService()

    def insert_data_batch(self, data_batch: list[dict]):
        data: DataFrame = self.convert_json_to_polars(data_batch)
        self.data_service.insert_data_batch(data)

    def convert_json_to_polars(self, json_data) -> DataFrame:
        return self.data_service.data.from_dicts(json_data)