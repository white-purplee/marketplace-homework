import logging
import os
from contextlib import asynccontextmanager
from datetime import date
from typing import Annotated
from uuid import UUID

import grpc
import sqlalchemy as sa
from fastapi import FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator
from sqlalchemy.exc import SQLAlchemyError
from common.db import make_engine
from booking_service.flight_client import FlightClient
from booking_service.service import ApiError, BookingService, RecoveryWorker, rpc_error

log = logging.getLogger(__name__)


class CreateBooking(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    user_id: str = Field(min_length=1, max_length=128)
    flight_id: UUID
    passenger_name: str = Field(min_length=1, max_length=200)
    passenger_email: EmailStr = Field(max_length=254)
    seat_count: int = Field(gt=0, le=2147483647, strict=True)

    @field_validator("passenger_name", "user_id")
    @classmethod
    def nonempty(cls, value):
        if not value.strip():
            raise ValueError("Must not be blank")
        return value


def create_app(service=None, *, recovery=True):
    @asynccontextmanager
    async def lifespan(app):
        owned = service is None
        svc = service
        if owned:
            engine, lock_engine = make_engine(), make_engine(locks=True)
            svc = BookingService(engine, lock_engine, FlightClient())
        app.state.service = svc
        worker = RecoveryWorker(svc, float(os.getenv("RECOVERY_INTERVAL_SECONDS", "2"))) if recovery else None
        if worker:
            worker.start()
        yield
        if worker:
            worker.stop()
        if owned:
            svc.client.close()
            svc.engine.dispose()
            svc.lock_engine.dispose()

    app = FastAPI(title="Flight Booking", version="1.0.0", lifespan=lifespan)

    @app.exception_handler(ApiError)
    async def api_error(request, exc):
        content = {"code": exc.code, "message": exc.message}
        if exc.operation_id:
            content["operation_id"] = str(exc.operation_id)
        return JSONResponse(content=content, status_code=exc.status)

    @app.exception_handler(grpc.RpcError)
    async def grpc_error(request, exc):
        return await api_error(request, rpc_error(exc))

    @app.exception_handler(SQLAlchemyError)
    async def db_error(request, exc):
        log.error("booking_database_unavailable", exc_info=(type(exc), exc, exc.__traceback__))
        return JSONResponse(status_code=503, content={"code": "UNAVAILABLE", "message": "Booking database temporarily unavailable"})

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        return JSONResponse(status_code=422, content={"code": "INVALID_ARGUMENT", "message": "Request validation failed",
                                                     "details": jsonable_encoder(exc.errors(), custom_encoder={ValueError: str})})

    @app.get("/health/live")
    def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready(request: Request):
        with request.app.state.service.engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        return {"status": "ready"}

    @app.get("/flights")
    def search(request: Request, origin: Annotated[str, Query(pattern="^[A-Z]{3}$")],
               destination: Annotated[str, Query(pattern="^[A-Z]{3}$")], date: date | None = None):
        return {"items": request.app.state.service.client.search(origin, destination, date)}

    @app.get("/flights/{flight_id}")
    def get_flight(flight_id: UUID, request: Request):
        return request.app.state.service.client.get(flight_id)

    @app.post("/bookings", status_code=201)
    def create(body: CreateBooking, request: Request,
               idempotency_key: Annotated[str, Header(min_length=1, max_length=128)]):
        if not idempotency_key.strip():
            raise ApiError(422, "INVALID_ARGUMENT", "Idempotency-Key must not be blank")
        return request.app.state.service.create(body.model_dump(mode="json"), idempotency_key)

    @app.get("/bookings")
    def list_bookings(request: Request, user_id: Annotated[str, Query(min_length=1, max_length=128)]):
        if not user_id.strip():
            raise ApiError(422, "INVALID_ARGUMENT", "user_id must not be blank")
        return {"items": request.app.state.service.list(user_id)}

    @app.get("/bookings/{booking_id}")
    def get_booking(booking_id: UUID, request: Request):
        return request.app.state.service.get(booking_id)

    @app.post("/bookings/{booking_id}/cancel")
    def cancel(booking_id: UUID, request: Request):
        return request.app.state.service.cancel(booking_id)

    @app.get("/operations/{operation_id}")
    def get_operation(operation_id: UUID, request: Request):
        op = request.app.state.service.operation(operation_id)
        return {k: op[k] for k in ("id", "booking_id", "kind", "state", "error_code", "error_message", "created_at", "updated_at")}

    return app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
app = create_app()
