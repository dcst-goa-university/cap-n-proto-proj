from lib.datalib.service import DataService
from iot.service import IoTService
import time

def main():
    data_service = DataService()
    iot_service = IoTService(
        data_service=data_service,
        input_pin=17,
        post_url="http://localhost:8000/server/batch/insert",  # JSON
        post_interval=1,
        max_records=10,  # smaller for frequent pushes
        grpc_server="localhost:50051"
    )

    transports = {
        "http" : iot_service.push_on_max_http(),
        "grpc" : iot_service.push_on_max_grpc(),
        "cnpnp" : iot_service.push_on_max_cnpnp(),
        "cnpnp_grpc" : iot_service.push_on_max_cnpnp_grpc(),
        "arrow" : iot_service.push_on_max_arrow(),
        "arrow_grpc" : iot_service.push_on_max_arrow_grpc()
    }

    for transport in transports.keys():
        i = 0
        while i<10:
            iot_service.monitor_and_store()
            transports[transport]
            i += 1



if __name__ == "__main__":
    main()
