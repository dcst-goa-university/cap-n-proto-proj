import polars as pl
from cnp_test_types import SensorDataDTO

PYDANTIC_TO_POLARS = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.Utf8,
    bool: pl.Boolean,
}

schema = {
    name: PYDANTIC_TO_POLARS[field.annotation]
    for name, field in SensorDataDTO.model_fields.items()
}

class DataService:
    def __init__(self):
        self.schema = schema
        self.data_frame = pl.DataFrame(schema=self.schema)