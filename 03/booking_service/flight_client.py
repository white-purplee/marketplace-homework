import os
import grpc
from flight.v1 import flight_pb2 as pb, flight_pb2_grpc as rpc
from booking_service.resilience import configured_interceptor


def flight_json(f):
    return {
        "id": f.id, "flight_number": f.flight_number, "airline": f.airline,
        "origin": f.origin, "destination": f.destination,
        "departure_at": f.departure_at.ToJsonString(), "arrival_at": f.arrival_at.ToJsonString(),
        "total_seats": f.total_seats, "available_seats": f.available_seats,
        "price_minor": f.price_minor, "status": pb.FlightStatus.Name(f.status),
    }


class FlightClient:
    def __init__(self, target=None, interceptor=None):
        self.base_channel = grpc.insecure_channel(
            target or os.getenv("FLIGHT_GRPC_ADDRESS", "flight-service:50051"),
            options=[("grpc.enable_retries", 0)],
        )
        self.channel = grpc.intercept_channel(self.base_channel, interceptor or configured_interceptor())
        self.stub = rpc.FlightServiceStub(self.channel)

    def get(self, flight_id):
        return flight_json(self.stub.GetFlight(pb.GetFlightRequest(flight_id=str(flight_id))).flight)

    def search(self, origin, destination, departure_date=None):
        request = pb.SearchFlightsRequest(origin=origin, destination=destination)
        if departure_date:
            request.departure_date.CopyFrom(pb.Date(year=departure_date.year, month=departure_date.month, day=departure_date.day))
        return [flight_json(f) for f in self.stub.SearchFlights(request).flights]

    def reserve(self, flight_id, seat_count, booking_id):
        return self.stub.ReserveSeats(pb.ReserveSeatsRequest(
            flight_id=str(flight_id), seat_count=seat_count, booking_id=str(booking_id)))

    def release(self, booking_id):
        return self.stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=str(booking_id)))

    def close(self):
        self.channel.close()
