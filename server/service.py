from lib.datalib.service import DataService
from lib.datalib.model import pydantic_to_polars_schema, SensorDataDTO
from polars import DataFrame
import capnp
import pyarrow as pa
import pyarrow.ipc as ipc
from io import BytesIO
from cnpap.sensor_pb2 import SensorDataBatch
import os
import logging

logger = logging.getLogger(__name__)

# Load Cap'n Proto schema
HERE = os.path.dirname(__file__)
CAPNP_PATH = os.path.join(HERE, '../cnpap/sensor.capnp')
sensor_data = capnp.load(CAPNP_PATH)


class ServerService():
    def __init__(self):
        self.data_service = DataService()

    # ---------------- JSON / HTTP ----------------
    def insert_data_batch(self, data_batch: list[dict]):
        df: DataFrame = self.convert_json_to_polars(data_batch)
        self.data_service.insert_data_batch(df)

    def convert_json_to_polars(self, json_data) -> DataFrame:
        return DataFrame(json_data, schema=pydantic_to_polars_schema(SensorDataDTO))


    # ---------------- Cap'n Proto over HTTP ----------------
    def insert_capnp_batch(self, payload: bytes):
        try:
            with sensor_data.SensorBatch.from_bytes(payload) as batch:
                data_batch = [
                    {"timestamp": rec.timestamp, "value": rec.value}
                    for rec in batch.records
                ]
            self.insert_data_batch(data_batch)
        except Exception:
            logger.exception("Cap'n Proto decode failed")
            raise
        
        
    # ---------------- Arrow over HTTP ----------------
    def insert_arrow_batch(self, payload: bytes):
        buf = BytesIO(payload)
        reader = ipc.open_stream(buf)
        table = reader.read_all()
        data_batch = [{"timestamp": table["timestamp"][i].as_py(), "value": table["value"][i].as_py()}
                      for i in range(table.num_rows)]
        self.insert_data_batch(data_batch)

    # ---------------- gRPC ----------------
    def insert_grpc_batch(self, request: SensorDataBatch):
        data_batch = [{"timestamp": r.timestamp, "value": r.value} for r in request.data]
        self.insert_data_batch(data_batch)
