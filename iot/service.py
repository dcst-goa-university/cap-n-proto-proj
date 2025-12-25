from lib.datalib.service import DataService
from lib.datalib.model import SensorDataDTO
from polars import DataFrame
import time
import requests
import logging
import capnp
import grpc
import pyarrow as pa
import pyarrow.ipc as ipc
from io import BytesIO
from cnpap import sensor_pb2, sensor_pb2_grpc
from cnpap.sensor_pb2 import SensorData, SensorDataBatch
from cnpap.sensor_pb2_grpc import SensorServiceStub
import os
from lib.timelog.service import timeit_if_returned


try:
    import RPi.GPIO as GPIO
except (ImportError, RuntimeError):
    from .gpio_mock import GPIO



HERE = os.path.dirname(__file__)
CAPNP_PATH = os.path.join(HERE, '../cnpap/sensor.capnp')
sensor_data = capnp.load(CAPNP_PATH)



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

class IoTService:
    def __init__(self, data_service: DataService, input_pin:int, post_url:str, post_interval:int, max_records:int, grpc_server:str = None):
        self.data_service = data_service
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(input_pin, GPIO.IN)
        self.input_pin = input_pin
        self.post_url = post_url
        self.post_interval = post_interval
        self.max_records = max_records

        # gRPC setup
        self.grpc_stub = None
        if grpc_server:
            self.grpc_channel = grpc.insecure_channel(grpc_server)
            self.grpc_stub = SensorServiceStub(self.grpc_channel)

    
    # ---------------- Sensor read & store ----------------
    def __read_sensor_data(self):
        return GPIO.input(self.input_pin)

    def __create_sensor_record(self, sensor_value) -> SensorDataDTO:
        timestamp = time.time()
        return SensorDataDTO(timestamp=timestamp, value=sensor_value)

    def __insert_sensor_data(self, sensor_record: SensorDataDTO):
        self.data_service.add_sensor_data(sensor_record)

    def monitor_and_store(self):
        sensor_value = self.__read_sensor_data()
        sensor_record = self.__create_sensor_record(sensor_value)
        self.__insert_sensor_data(sensor_record)

    # ---------------- Data management ----------------
    def __clear_data(self):
        self.data_service.delete_all_data()

    def __check_if_max_data_reached(self) -> bool:
        return self.data_service.get_data_count() >= self.max_records

    def __get_data_batch(self) -> list[dict]:
        return self.data_service.get_recent_batch_data(self.max_records)

    # ---------------- Serialization ----------------
    def __serialize_capnp(self, data_batch: list[dict]) -> bytes:
        batch = sensor_data.SensorBatch.new_message()
        records = batch.init("records", len(data_batch))
        for i, row in enumerate(data_batch):
            records[i].timestamp = row["timestamp"]
            records[i].value = row["value"]
        return batch.to_bytes()

    def __serialize_arrow(self, data_batch: list[dict]) -> bytes:
        table = pa.table({
            "timestamp": [r["timestamp"] for r in data_batch],
            "value": [r["value"] for r in data_batch],
        })
        sink = BytesIO()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue()

    # ---------------- HTTP Post ----------------
    def __post_data(self, data_batch: list[dict]):
        response = requests.post(self.post_url, json=data_batch)
        return response.status_code == 200

    # ---------------- gRPC Send ----------------
    def __grpc_send(self, batch: list[dict]) -> bool:
        if not self.grpc_stub:
            logger.warning("gRPC stub not initialized.")
            return False
        msg = SensorDataBatch(
            data=[SensorData(timestamp=float(d["timestamp"]), value=int(d["value"])) for d in batch]
        )
        try:
            self.grpc_stub.SendData(msg)
            return True
        except Exception as e:
            return False

    # ---------------- Push Methods ----------------
    @timeit_if_returned
    def push_on_max_http(self):
        if self.__check_if_max_data_reached():
            batch = self.__get_data_batch()
            if self.__post_data(batch):
                self.__clear_data()
                return True

    @timeit_if_returned
    def push_on_max_grpc(self):
        if self.__check_if_max_data_reached():
            batch = self.__get_data_batch()
            if self.__grpc_send(batch):
                self.__clear_data()
                return True

    @timeit_if_returned
    def push_on_max_cnpnp(self):
        if self.__check_if_max_data_reached():
            batch = self.__get_data_batch()
            payload = self.__serialize_capnp(batch)
            response = requests.post(
                self.post_url + "/capnp",
                data=payload,
                headers={"Content-Type": "application/x-capnp"},
            )
            if response.status_code == 200:
                self.__clear_data()
                return True

    @timeit_if_returned
    def push_on_max_cnpnp_grpc(self):
        if self.__check_if_max_data_reached() and self.grpc_stub:
            batch = self.__get_data_batch()
            # Just reuse the standard protobuf batch; your Cap'n Proto data is identical
            if self.__grpc_send(batch):
                self.__clear_data()
                return True

    @timeit_if_returned
    def push_on_max_arrow(self):
        if self.__check_if_max_data_reached():
            batch = self.__get_data_batch()
            payload = self.__serialize_arrow(batch)
            response = requests.post(
                self.post_url + "/arrow",
                data=payload,
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            )
            if response.status_code == 200:
                self.__clear_data()
                return True
    
    @timeit_if_returned
    def push_on_max_arrow_grpc(self):
        if self.__check_if_max_data_reached() and self.grpc_stub:
            batch = self.__get_data_batch()
            # Simplest approach: convert to protobuf
            if self.__grpc_send(batch):
                self.__clear_data()
                return True
            

    # ---------------- Getter Send ----------------

    def set_max_records(self, new_max_records:int):
        self.max_records = new_max_records
