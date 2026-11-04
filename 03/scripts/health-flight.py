import os
import grpc
from sqlalchemy import text
from common.db import make_engine
from flight.v1 import flight_pb2 as pb, flight_pb2_grpc as rpc

engine = make_engine()
with engine.connect() as connection:
    connection.execute(text("SELECT 1"))
engine.dispose()
with grpc.insecure_channel("localhost:50051") as channel:
    rpc.FlightServiceStub(channel).GetFlight(
        pb.GetFlightRequest(flight_id="11111111-1111-4111-8111-111111111111"),
        metadata=[("x-api-key", os.environ["FLIGHT_API_KEY"])], timeout=4,
    )
