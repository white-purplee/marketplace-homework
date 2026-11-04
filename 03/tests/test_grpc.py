from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
import grpc
import pytest
from flight.v1 import flight_pb2 as pb, flight_pb2_grpc as rpc
from flight_service.server import AuthInterceptor, Handler, build_server
from booking_service.flight_client import FlightClient
from booking_service.resilience import ResilienceInterceptor

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("method,message", [
    ("SearchFlights", pb.SearchFlightsRequest(origin="SVO", destination="LED")),
    ("GetFlight", pb.GetFlightRequest(flight_id=str(uuid4()))),
    ("ReserveSeats", pb.ReserveSeatsRequest(flight_id=str(uuid4()), booking_id=str(uuid4()), seat_count=1)),
    ("ReleaseReservation", pb.ReleaseReservationRequest(booking_id=str(uuid4()))),
    ("UpdateFlight", pb.UpdateFlightRequest(flight_id=str(uuid4()), price_minor=1)),
])
def test_authentication_on_every_method(stack, method, message):
    server, port = build_server(stack[0], "test-secret", "127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            call = getattr(rpc.FlightServiceStub(channel), method)
            for metadata in [[], [("x-api-key", "wrong")], [("x-api-key", "test-secret"), ("x-api-key", "test-secret")]]:
                with pytest.raises(grpc.RpcError) as error:
                    call(message, metadata=metadata, timeout=3)
                assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
    finally:
        server.stop(0).wait()


def test_real_grpc_retry_after_lost_reserve_response(stack):
    flight, flight_id, *_ = stack
    class LoseFirstResponse(Handler):
        calls = 0
        def ReserveSeats(self, request, context):
            response = super().ReserveSeats(request, context)
            self.calls += 1
            if self.calls == 1:
                context.abort(grpc.StatusCode.UNAVAILABLE, "Response lost after commit")
            return response
    handler = LoseFirstResponse(flight)
    server = grpc.server(ThreadPoolExecutor(max_workers=4), interceptors=[AuthInterceptor("test-secret")])
    rpc.add_FlightServiceServicer_to_server(handler, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    client = FlightClient(f"127.0.0.1:{port}", ResilienceInterceptor("test-secret", initial_delay=0.01))
    try:
        assert client.get(flight_id)["id"] == str(flight_id)
        client.reserve(flight_id, 2, uuid4())
        assert handler.calls == 2
        assert client.get(flight_id)["available_seats"] == 8
        assert client.search("SVO", "LED")
    finally:
        client.close()
        server.stop(0).wait()


def test_grpc_validates_calendar_dates_and_business_errors(stack):
    server, port = build_server(stack[0], "test-secret", "127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = rpc.FlightServiceStub(channel)
            metadata = [("x-api-key", "test-secret")]
            with pytest.raises(grpc.RpcError) as error:
                stub.SearchFlights(pb.SearchFlightsRequest(origin="SVO", destination="LED",
                    departure_date=pb.Date(year=2027, month=2, day=30)), metadata=metadata, timeout=3)
            assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
            with pytest.raises(grpc.RpcError) as error:
                stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=str(stack[1]), booking_id=str(uuid4()), seat_count=11),
                                  metadata=metadata, timeout=3)
            assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    finally:
        server.stop(0).wait()


def test_all_sentinels_unavailable_falls_back_within_rpc_deadline(stack, monkeypatch):
    import socket
    from flight_service.cache import FlightCache
    from flight_service.service import FlightService
    # Reserve an unused, non-listening local port: actual connection failures,
    # including redis-py's retry defaults, rather than a mocked Redis exception.
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        port = unused.getsockname()[1]
        monkeypatch.setenv("REDIS_SENTINELS", ",".join([f"127.0.0.1:{port}"] * 3))
        flight = FlightService(stack[0].engine, FlightCache())
        server, grpc_port = build_server(flight, "test-secret", "127.0.0.1:0")
        server.start()
        client = FlightClient(f"127.0.0.1:{grpc_port}", ResilienceInterceptor("test-secret", attempts=1))
        try:
            assert client.get(stack[1])["id"] == str(stack[1])
        finally:
            client.close()
            server.stop(0).wait()
