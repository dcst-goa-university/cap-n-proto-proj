from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from server.controller import ServerController
from server.service import ServerService
from uvicorn import run
import threading
from server.grpc_server import serve as grpc_serve

def create_app() -> FastAPI:
    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    server_service = ServerService()
    server_controller = ServerController(app.router, server_service)
    server_controller.register_routes()

    return app

if __name__ == "__main__":
    # Start gRPC server in a separate thread
    threading.Thread(target=grpc_serve, daemon=True).start()

    # Start FastAPI HTTP server
    app = create_app()
    run(app, host="0.0.0.0", port=8000)
