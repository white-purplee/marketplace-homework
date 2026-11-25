import csv
import io
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta

import boto3
import clickhouse_connect
import psycopg
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("analytics")
PG = os.getenv("PG_DSN", "postgresql://cinema:cinema@postgres/cinema")
BUCKET = "movie-analytics"
lock = threading.Lock()
DAY = timedelta(days=1)


def clickhouse():
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "clickhouse"), port=int(os.getenv("CH_PORT", "8123")),
        username="cinema", password="cinema", autogenerate_session_id=False,
    )


def migrate():
    with psycopg.connect(PG) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS metrics (
            day date NOT NULL, metric text NOT NULL, movie_id text NOT NULL DEFAULT '',
            value double precision, calculated_at timestamptz NOT NULL,
            PRIMARY KEY (day, metric, movie_id))""")
        db.execute("""CREATE TABLE IF NOT EXISTS export_jobs (
            day date PRIMARY KEY, done boolean NOT NULL DEFAULT false)""")


def recalculate(day):
    started = time.monotonic()
    log.info("Starting calculation for %s", day)
    params = {"day": day}
    with clickhouse() as ch:
        count, dau, avg_seconds, conversion = ch.query("""
            SELECT count(), uniqExact(user_id),
                   avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED'),
                   countIf(event_type = 'VIEW_FINISHED') / nullIf(countIf(event_type = 'VIEW_STARTED'), 0)
            FROM events FINAL WHERE toDate(timestamp) = {day:Date}
        """, parameters=params).first_row
        top = ch.query("""
            SELECT movie_id, count() AS views FROM events FINAL
            WHERE toDate(timestamp) = {day:Date} AND event_type = 'VIEW_STARTED'
            GROUP BY movie_id ORDER BY views DESC, movie_id LIMIT 10
        """, parameters=params).result_rows
        cohorts = ch.query("""
            SELECT dateDiff('day', first_day, {day:Date}) AS age, avg(toFloat64(active))
            FROM (
                SELECT user_id, min(toDate(timestamp)) AS first_day
                FROM events FINAL
                WHERE event_type = 'VIEW_STARTED' AND toDate(timestamp) <= {day:Date}
                GROUP BY user_id HAVING first_day >= subtractDays({day:Date}, 7)
            ) AS cohorts
            LEFT JOIN (
                SELECT DISTINCT user_id, toUInt8(1) AS active FROM events FINAL
                WHERE toDate(timestamp) = {day:Date}
            ) AS activity USING (user_id)
            GROUP BY age SETTINGS join_use_nulls = 0
        """, parameters=params).result_rows
        retention = [None] * 8
        for age, fraction in cohorts:
            retention[age] = fraction
        at = datetime.now(UTC)
        ch.insert("daily", [(day, dau, avg_seconds, conversion, top, retention, at)],
                  column_names=["day", "dau", "avg_seconds", "conversion", "top_movies",
                                "retention", "calculated_at"])

    values = [("dau", "", dau), ("avg_seconds", "", avg_seconds), ("conversion", "", conversion)]
    values.extend((f"retention_d{i}", "", value) for i, value in enumerate(retention))
    values.extend(("movie_views", movie, views) for movie, views in top)
    for attempt in range(3):
        try:
            with psycopg.connect(PG, connect_timeout=5) as db:
                old = db.execute("SELECT metric, movie_id, value FROM metrics WHERE day=%s",
                                 (day,)).fetchall()
                # Replace the whole day's ranking: films can also drop out of the top ten.
                db.execute("DELETE FROM metrics WHERE day=%s", (day,))
                with db.cursor() as cursor:
                    cursor.executemany("INSERT INTO metrics VALUES (%s,%s,%s,%s,%s)",
                                       [(day, name, movie, v, at) for name, movie, v in values])
                if day < datetime.now(UTC).date():
                    changed = set(old) != set(values)
                    db.execute("""INSERT INTO export_jobs(day) VALUES (%s)
                                  ON CONFLICT(day) DO UPDATE SET done=false WHERE %s""",
                               (day, changed))
            break
        except psycopg.OperationalError:
            log.exception("PostgreSQL write failed on attempt %s", attempt + 1)
            if attempt == 2:
                raise
            time.sleep(2**attempt)
    elapsed = round(time.monotonic() - started, 3)
    log.info("Finished %s: %s events in %s seconds", day, count, elapsed)
    return {"date": day, "events": count, "seconds": elapsed}


def export(day):
    with psycopg.connect(PG, connect_timeout=5) as db:
        rows = db.execute("""SELECT day, metric, movie_id, value, calculated_at FROM metrics
                             WHERE day=%s ORDER BY metric, movie_id""", (day,)).fetchall()
        if not rows:
            raise HTTPException(404, "No calculated metrics for this date")
        db.execute("""INSERT INTO export_jobs(day) VALUES (%s)
                      ON CONFLICT(day) DO UPDATE SET done=false""", (day,))
    # The pending job is committed before attempting S3, so the scheduler can retry.
    out = io.StringIO(newline="")
    writer = csv.writer(out)
    writer.writerow(["date", "metric", "movie_id", "value", "calculated_at"])
    writer.writerows(rows)
    s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
                      region_name="us-east-1", aws_access_key_id="cinema",
                      aws_secret_access_key="cinema-secret",
                      config=Config(connect_timeout=4, read_timeout=10,
                                    s3={"addressing_style": "path"}))
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        s3.create_bucket(Bucket=BUCKET)
    key = f"daily/{day}/aggregates.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=out.getvalue().encode(), ContentType="text/csv")
    with psycopg.connect(PG, connect_timeout=5) as db:
        db.execute("UPDATE export_jobs SET done=true WHERE day=%s", (day,))
    log.info("Exported %s", key)
    return {"key": key}


def schedule(stop):
    interval = max(1, int(os.getenv("AGG_INTERVAL_SECONDS", "60")))
    while not stop.is_set():
        try:
            today = datetime.now(UTC).date()
            with psycopg.connect(PG, connect_timeout=5) as db:
                last = db.execute("SELECT max(day) FROM metrics WHERE day <= %s", (today,)).fetchone()[0]
            # Refresh demo dates and catch up after downtime.
            day = min(last or today, today - 8 * DAY)
            while day <= today and not stop.is_set():
                with lock:
                    recalculate(day)
                day += DAY
            with psycopg.connect(PG, connect_timeout=5) as db:
                pending = db.execute("SELECT day FROM export_jobs WHERE NOT done ORDER BY day").fetchall()
            for (day,) in pending:
                if stop.is_set():
                    break
                try:
                    with lock:
                        export(day)
                except Exception:
                    log.exception("Export for %s remains pending", day)
        except Exception:
            log.exception("Scheduled cycle failed")
        stop.wait(interval)


@asynccontextmanager
async def lifespan(app):
    migrate()
    stop = threading.Event()
    worker = threading.Thread(target=schedule, args=(stop,), daemon=True)
    worker.start()
    yield
    stop.set()
    worker.join(timeout=10)


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/recalculate/{day}")
def recalculate_date(day: date):
    with lock:
        return recalculate(day)


@app.post("/export/{day}")
def export_date(day: date):
    try:
        with lock:
            return export(day)
    except HTTPException:
        raise
    except Exception:
        log.exception("Export failed for %s", day)
        raise HTTPException(503, "Export failed")
