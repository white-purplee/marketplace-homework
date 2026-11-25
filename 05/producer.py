import json
import logging
import os
import random
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, time as clock, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI, HTTPException
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from movie_pb2 import MovieEvent

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("movie-service")
BROKERS = os.getenv("BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092")
REGISTRY = os.getenv("REGISTRY", "http://registry:8081")
TOPIC = "movie-events"


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    event_id: UUID
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: str
    timestamp: AwareDatetime
    device_type: str
    session_id: str = Field(min_length=1)
    progress_seconds: int = Field(ge=0, le=2**32 - 1, strict=True)

    @field_validator("event_type")
    @classmethod
    def valid_action(cls, value):
        MovieEvent.EventType.Value(value)
        return value

    @field_validator("device_type")
    @classmethod
    def valid_device(cls, value):
        MovieEvent.DeviceType.Value(value)
        return value


def register_schema():
    subject = f"{REGISTRY}/subjects/{TOPIC}-value"
    data = json.dumps({"schemaType": "PROTOBUF", "schema": Path("movie.proto").read_text()}).encode()
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    with urlopen(Request(subject + "/versions", data, headers), timeout=10) as reply:
        schema_id = json.load(reply)["id"]
    with urlopen(Request(subject, data, headers), timeout=10) as reply:
        version = json.load(reply)["version"]
    log.info("Registered schema id=%s version=%s", schema_id, version)
    return [("schema-id", str(schema_id).encode()), ("schema-version", str(version).encode())]


def publish(event):
    message = MovieEvent(
        event_id=str(event.event_id), user_id=event.user_id, movie_id=event.movie_id,
        event_type=MovieEvent.EventType.Value(event.event_type),
        device_type=MovieEvent.DeviceType.Value(event.device_type),
        timestamp=(event.timestamp - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(milliseconds=1),
        session_id=event.session_id, progress_seconds=event.progress_seconds,
    )
    delivered = threading.Event()
    errors = []

    def on_delivery(error, _message):
        if error:
            errors.append(str(error))
        delivered.set()

    app.state.producer.produce(TOPIC, key=event.user_id, value=message.SerializeToString(),
                               headers=app.state.schema, on_delivery=on_delivery)
    deadline = time.monotonic() + 20
    while not delivered.is_set():
        if time.monotonic() >= deadline:
            raise TimeoutError("Kafka acknowledgement timed out")
        app.state.producer.poll(0.1)
    if errors:
        raise RuntimeError(errors[0])
    log.info("Published %s %s %s", event.event_id, event.event_type, event.timestamp.isoformat())


def session(user, start, seed):
    rng = random.Random(seed)
    session_id = str(uuid5(NAMESPACE_URL, seed))
    movie = f"movie-{rng.randint(1, 12)}"
    device = rng.choice(MovieEvent.DeviceType.keys())
    length = rng.randint(600, 3500)
    actions = [("SEARCHED", 0, 0), ("VIEW_STARTED", 1, 0)]
    if rng.random() < 0.6:
        actions += [("VIEW_PAUSED", 121, 120), ("VIEW_RESUMED", 151, 120)]
    if rng.random() < 0.8:
        actions.append(("VIEW_FINISHED", length + 31, length))
        if rng.random() < 0.5:
            actions.append(("LIKED", length + 32, length))
    for kind, delay, progress in actions:
        yield Event(event_id=uuid5(NAMESPACE_URL, seed + kind), user_id=user, movie_id=movie,
                    event_type=kind, timestamp=start + timedelta(seconds=delay),
                    device_type=device, session_id=session_id, progress_seconds=progress)


def generate(stop):
    # Stable IDs let a restart safely replay the nine-day demonstration.
    today = datetime.now(UTC).date()
    try:
        for offset in range(8, -1, -1):
            day = today - timedelta(days=offset)
            for age in (0, 1, 3, 7):
                cohort = day - timedelta(days=age)
                for index in range({0: 4, 1: 3, 3: 2, 7: 1}[age]):
                    user = f"user-{cohort}-{index}"
                    start = datetime.combine(day, clock(8 + index), UTC)
                    if start + timedelta(hours=1) > datetime.now(UTC):
                        continue
                    for event in session(user, start, f"{day}/{user}"):
                        if stop.is_set():
                            return
                        publish(event)
    except Exception:
        log.exception("Demo history interrupted")

    while not stop.is_set():
        try:
            now = datetime.now(UTC)
            user = f"user-{now.date()}-{random.randrange(4)}"
            for event in session(user, now - timedelta(hours=1), str(uuid4())):
                publish(event)
        except Exception:
            log.exception("Generator publication failed")
        stop.wait(5)


@asynccontextmanager
async def lifespan(app):
    admin = AdminClient({"bootstrap.servers": BROKERS})
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=2,
                     config={"min.insync.replicas": "1"})
    try:
        admin.create_topics([topic], request_timeout=20)[TOPIC].result()
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    app.state.schema = register_schema()
    app.state.producer = Producer({
        "bootstrap.servers": BROKERS, "enable.idempotence": True, "acks": "all",
        "retry.backoff.ms": 100, "retry.backoff.max.ms": 2000, "delivery.timeout.ms": 15000,
    })
    stop = threading.Event()
    worker = threading.Thread(target=generate, args=(stop,), daemon=True)
    if os.getenv("GENERATE_EVENTS", "1") == "1":
        worker.start()
    yield
    stop.set()
    if worker.is_alive():
        worker.join(timeout=20)
    app.state.producer.flush(5)


app = FastAPI(title="Movie Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=201)
def events(event: Event):
    try:
        publish(event)
    except (KafkaException, BufferError, TimeoutError, RuntimeError) as exc:
        log.exception("Publication failed for %s", event.event_id)
        raise HTTPException(503, "Kafka publication failed") from exc
    return {"event_id": str(event.event_id)}
