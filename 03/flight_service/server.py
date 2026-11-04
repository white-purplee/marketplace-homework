import hmac
import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from sqlalchemy.exc import OperationalError
from common.db import make_engine
from common.errors import DomainError, invalid
from flight.v1 import flight_pb2 as pb, flight_pb2_grpc as rpc
from flight_service.cache import FlightCache
from flight_service.service import FlightService

log = logging.getLogger(__name__)


def timestamp(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    result = Timestamp()
    result.FromDatetime(value.astimezone(timezone.utc))
    return result


def flight_message(row):
    fields = {k: row[k] for k in (
        "id", "flight_number", "airline", "origin", "destination", "total_seats", "available_seats", "price_minor")}
    return pb.Flight(**fields, departure_at=timestamp(row["departure_at"]),
                     arrival_at=timestamp(row["arrival_at"]), status=pb.FlightStatus.Value(row["status"]))


def reservation_message(row):
    return pb.SeatReservation(
        id=str(row["id"]), booking_id=str(row["booking_id"]), flight_id=str(row["flight_id"]),
        seat_count=row["seat_count"], status=pb.ReservationStatus.Value(row["status"]),
        created_at=timestamp(row["created_at"]), updated_at=timestamp(row["updated_at"]),
    )


class AuthInterceptor(grpc.ServerInterceptor):
    def __init__(self, key):
        if not key:
            raise ValueError("FLIGHT_API_KEY is required")
        self.key = key.encode()

    def intercept_service(self, continuation, details):
        values = [v for k, v in details.invocation_metadata if k == "x-api-key"]
        if len(values) == 1 and hmac.compare_digest(values[0].encode(), self.key):
            return continuation(details)

        def reject(request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid service credentials")
        return grpc.unary_unary_rpc_method_handler(reject)


class Handler(rpc.FlightServiceServicer):
    def __init__(self, service):
        self.service = service

    @staticmethod
    def _run(context, callback):
        try:
            return callback()
        except DomainError as exc:
            context.abort(exc.code, exc.message)
        except OperationalError:
            log.exception("flight_database_unavailable")
            context.abort(grpc.StatusCode.UNAVAILABLE, "Flight database temporarily unavailable")
        except Exception:
            log.exception("flight_unexpected_error")
            context.abort(grpc.StatusCode.INTERNAL, "Internal flight service error")

    def SearchFlights(self, request, context):
        def search():
            when = None
            if request.HasField("departure_date"):
                try:
                    when = date(request.departure_date.year, request.departure_date.month, request.departure_date.day)
                except ValueError:
                    invalid("Invalid calendar date")
            return pb.SearchFlightsResponse(flights=[flight_message(f) for f in self.service.search(
                request.origin, request.destination, when)])
        return self._run(context, search)

    def GetFlight(self, request, context):
        return self._run(context, lambda: pb.GetFlightResponse(flight=flight_message(self.service.get(request.flight_id))))

    def ReserveSeats(self, request, context):
        return self._run(context, lambda: pb.ReserveSeatsResponse(reservation=reservation_message(
            self.service.reserve(request.flight_id, request.seat_count, request.booking_id))))

    def ReleaseReservation(self, request, context):
        return self._run(context, lambda: pb.ReleaseReservationResponse(**self.service.release(request.booking_id)))

    def UpdateFlight(self, request, context):
        return self._run(context, lambda: pb.UpdateFlightResponse(flight=flight_message(
            self.service.update(request.flight_id, request.price_minor))))


def build_server(service, key, address="[::]:50051"):
    server = grpc.server(ThreadPoolExecutor(max_workers=16), interceptors=[AuthInterceptor(key)])
    rpc.add_FlightServiceServicer_to_server(Handler(service), server)
    port = server.add_insecure_port(address)
    if not port:
        raise RuntimeError(f"Cannot bind {address}")
    return server, port


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    engine = make_engine()
    server, _ = build_server(FlightService(engine, FlightCache()), os.environ["FLIGHT_API_KEY"])
    server.start()
    log.info("flight_server_started port=50051")
    signal.signal(signal.SIGTERM, lambda *_: server.stop(5))
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(5).wait()
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
