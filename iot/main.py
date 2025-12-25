import logging

def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler("experimentlog.txt", mode="w")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)


setup_logging() 

from .module import IoTModule



iot = IoTModule()

repition = 230
max_records = [10**i for i in range(1, 4)]

if __name__ == "__main__":
    TransportClasses = iot.return_transport_classes()
    for max_record in max_records:
        for transportClass in TransportClasses:
            logging.info(f'RUNNING: {transportClass} transport Experiment for max records of {max_record} at repition {repition}')
            experimentNo = 0
            while experimentNo < repition:
                iot.run(max_record, [transportClass])
                experimentNo += 1



