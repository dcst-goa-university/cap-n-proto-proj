from lib.datalib.service import DataService
from iot.service import IoTService
import time
import logging

MAX_RECORDS = 1

class IoTModule:
    def __init__(self):
        self.data_service = DataService()
        self.iot_service = IoTService(
            data_service=self.data_service,
            input_pin=17,
            post_url="http://localhost:8000/server/batch/insert",  # JSON
            post_interval=1,
            max_records=MAX_RECORDS,  # smaller for frequent pushes
            grpc_server="localhost:50051"
        )
        self.transports = {
            "http": self.iot_service.push_on_max_http,
            "grpc": self.iot_service.push_on_max_grpc,
            "cnpnp": self.iot_service.push_on_max_cnpnp,
            "cnpnp_grpc": self.iot_service.push_on_max_cnpnp_grpc,
            "arrow": self.iot_service.push_on_max_arrow,
            "arrow_grpc": self.iot_service.push_on_max_arrow_grpc
        }

    def return_transport_classes(self):
        return list(self.transports.keys())
    
    def return_transport_classes_len(self):
        return len(self.transports)
    
    def run(self, records, transport_names):
        self.iot_service.set_max_records(records)
        for transport_name in transport_names:
            record_count = 0
            while record_count < records:
                self.iot_service.monitor_and_store()
                self.transports[transport_name]()
                record_count += 1
