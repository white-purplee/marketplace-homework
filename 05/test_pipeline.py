import json
import os
import time
from datetime import UTC, datetime, timedelta
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

import clickhouse_connect


def test_pipeline():
    event_id = uuid4()
    now = datetime.now(UTC)
    stamp = now.replace(microsecond=now.microsecond // 1000 * 1000)
    event = {
        "event_id": str(event_id), "user_id": f"test-{event_id}", "movie_id": "test-film",
        "event_type": "VIEW_PAUSED", "timestamp": stamp.isoformat(),
        "device_type": "TV", "session_id": f"session-{event_id}", "progress_seconds": 300,
    }
    url = os.getenv("MOVIE_URL", "http://producer:8000") + "/events"
    request = Request(url, json.dumps(event).encode(), {"Content-Type": "application/json"})
    with urlopen(request, timeout=25) as response:
        assert response.status == 201
        assert UUID(json.load(response)["event_id"]) == event_id

    expected = (event_id, event["user_id"], "test-film", "VIEW_PAUSED",
                (stamp - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(milliseconds=1), "TV", event["session_id"], 300)
    deadline = time.monotonic() + 60
    with clickhouse_connect.get_client(host=os.getenv("CH_HOST", "clickhouse"),
                                       port=int(os.getenv("CH_PORT", "8123")),
                                       username="cinema", password="cinema") as ch:
        while time.monotonic() < deadline:
            rows = ch.query("""
                SELECT event_id, user_id, movie_id, event_type, toUnixTimestamp64Milli(timestamp),
                       device_type, session_id, progress_seconds
                FROM events FINAL WHERE event_id = {id:UUID}
            """, parameters={"id": str(event_id)}).result_rows
            if rows:
                assert rows == [expected], (rows, expected)
                print("OK: HTTP -> Kafka -> ClickHouse. All eight fields match.")
                return
            time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {event_id} in ClickHouse")


if __name__ == "__main__":
    test_pipeline()
