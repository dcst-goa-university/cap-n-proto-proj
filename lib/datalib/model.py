from pydantic import BaseModel
import polars as pl 
from typing import Type

class SensorDataDTO(BaseModel):
    timestamp: float
    value: float

def pydantic_to_polars_schema(model: Type[BaseModel]) -> dict:
    type_mapping = {
        str: pl.Utf8,
        int: pl.Int64,
        float: pl.Float64,
        bool: pl.Boolean,
    }

    schema = {}
    for field_name, field_info in model.__annotations__.items():
        if field_info in type_mapping:
            schema[field_name] = type_mapping[field_info]
        else:
            raise TypeError(f"Unsupported field type: {field_info} for field '{field_name}'")
    return schema

