import polars
from .model import SensorDataDTO, pydantic_to_polars_schema


class DataService():
    def __init__(self):
        self.data = polars.DataFrame(schema=pydantic_to_polars_schema(SensorDataDTO))

    def add_sensor_data(self, sensor_data: SensorDataDTO):
        new_data = polars.DataFrame([sensor_data.model_dump()])
        self.data = polars.concat([self.data, new_data], how="vertical")
    
    def get_all_data(self) -> polars.DataFrame:
        return self.data
    
    def delete_all_data(self):
        self.data = polars.DataFrame(schema=pydantic_to_polars_schema(SensorDataDTO))

    def make_data_batch(self, batch_start: int, batch_size: int) -> polars.DataFrame:
        return self.data[batch_start:batch_start+batch_size]

    def get_recent_batch_data(self, batch_size: int) -> polars.DataFrame:
        return self.make_data_batch(0, batch_size).to_dicts()
    
    def insert_data_batch(self, data_batch: list[dict] | polars.DataFrame):
        if isinstance(data_batch, list):
            new_df = polars.DataFrame(data_batch, schema=pydantic_to_polars_schema(SensorDataDTO))
        else:  # already a Polars DataFrame
            new_df = data_batch
        self.data = polars.concat([new_df, self.data], how="vertical")

    def get_data_count(self) -> int:
        return self.data.height