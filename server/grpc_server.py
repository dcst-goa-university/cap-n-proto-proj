import grpc
from concurrent import futures
from cnpap import sensor_pb2, sensor_pb2_grpc
from server.service import ServerService
from google.protobuf.empty_pb2 import Empty

class SensorServiceServicer(sensor_pb2_grpc.SensorServiceServicer):
    def __init__(self, server_service: ServerService):
        self.server_service = server_service

    def SendData(self, request, context):
        self.server_service.insert_grpc_batch(request)
        return Empty()


def serve(grpc_port=50051):
    server_service = ServerService()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    sensor_pb2_grpc.add_SensorServiceServicer_to_server(SensorServiceServicer(server_service), server)
    server.add_insecure_port(f'[::]:{grpc_port}')
    print(f"gRPC server running on port {grpc_port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
