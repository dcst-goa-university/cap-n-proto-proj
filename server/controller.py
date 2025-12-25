from fastapi import APIRouter, Response, Body, Request
from typing import List
from server.service import ServerService

class ServerController():
    def __init__(self, router: APIRouter, server_service: ServerService):
        self.router = router
        self.server_service = server_service

    def register_routes(self):
        # JSON HTTP
        @self.router.post("/server/batch/insert")
        async def insert_data_batch(data_batch: List[dict] = Body(...)):
            self.server_service.insert_data_batch(data_batch)
            return Response(status_code=200)

        # Cap'n Proto HTTP
        @self.router.post("/server/batch/insert/capnp")
        async def insert_capnp_batch(request: Request):
            payload = await request.body()
            self.server_service.insert_capnp_batch(payload)
            return Response(status_code=200)

        # Arrow HTTP
        @self.router.post("/server/batch/insert/arrow")
        async def insert_arrow_batch(request: Request):
            payload = await request.body()
            self.server_service.insert_arrow_batch(payload)
            return Response(status_code=200)
        
        @self.router.get("/server/data")
        async def get_data():
            """
            Returns all stored sensor data as JSON
            """
            # Polars DataFrame to list of dicts
            df = self.server_service.data_service.data
            if df is None or df.is_empty():
                return []
            return df.to_dicts()
