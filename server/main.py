from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .controller import ServerController
from .service import ServerService
from uvicorn import run

def create_app() -> FastAPI:
    app = FastAPI()

    # Set up CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Initialize services and controllers
    server_service = ServerService()
    server_controller = ServerController(app.router, server_service)
    server_controller.register_routes()

    return app

if __name__ == "__main__":
    app = create_app()
    run(app, host="0.0.0.0", port=8000)