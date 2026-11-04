"""Exercise the running REST API using only the Python standard library."""
import argparse
import json
import urllib.request
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    args = parser.parse_args()
    def call(path, body=None, key=None):
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(args.url + path, headers=headers,
            data=None if body is None else json.dumps(body).encode())
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)
    flights = call("/flights?origin=SVO&destination=LED")["items"]
    assert flights, "No SVO-LED flights"
    flight = flights[0]
    body = {"user_id": "demo-" + uuid4().hex, "flight_id": flight["id"], "passenger_name": "Demo Passenger",
            "passenger_email": "demo@example.com", "seat_count": 1}
    key = uuid4().hex
    booking = call("/bookings", body, key)
    repeated = call("/bookings", body, key)
    assert repeated["id"] == booking["id"], "Duplicate booking after retry"
    assert call("/flights/" + flight["id"])["available_seats"] == flight["available_seats"] - 1
    cancelled = call(f"/bookings/{booking['id']}/cancel", {})
    assert cancelled["status"] == "CANCELLED"
    call(f"/bookings/{booking['id']}/cancel", {})
    assert call("/flights/" + flight["id"])["available_seats"] == flight["available_seats"]
    print(json.dumps({"result": "passed", "booking_id": booking["id"], "checks": [
        "REST -> gRPC -> PostgreSQL", "idempotent creation", "seat inventory", "idempotent cancellation"]}, indent=2))


if __name__ == "__main__":
    main()
