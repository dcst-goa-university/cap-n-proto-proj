from fastapi import APIRouter, Response, Body
from typing import List
from server.service import ServerService


class ServerController():
    def __init__(self, router: APIRouter, server_service: ServerService):
        self.router = router
        self.server_service = server_service
    
    def register_routes(self):
        @self.router.post("/server/batch/insert")
        async def insert_data_batch(data_batch: List[dict] = Body(...)):
            self.server_service.insert_data_batch(data_batch)
            return Response(status_code=200)