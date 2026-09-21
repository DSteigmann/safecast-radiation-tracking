import csv
import itertools
import json
import os
import threading
import time
from pathlib import Path

try:
    import boto3
except ImportError:
    boto3 = None

from flask import Flask, jsonify, request
from kafka import KafkaProducer

app = Flask(__name__)

DEFAULT_KAFKA_BROKERS = "localhost:29092"
DEFAULT_TOPIC = "safecast-measurements"
DEFAULT_SUBMISSION_SPEED = float(os.getenv("SUBMISSION_SPEED", "1000"))
DEFAULT_MEASUREMENTS_FILE = (
    Path(__file__).resolve().with_name("measurements-out-short.csv")
)

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER", os.getenv("KAFKA_BROKERS", DEFAULT_KAFKA_BROKERS)
)
TOPIC = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
CSV_PATH = Path(os.getenv("MEASUREMENTS_FILE", DEFAULT_MEASUREMENTS_FILE))
PRODUCER_SETTINGS_PORT = int(os.getenv("PRODUCER_SETTINGS_PORT", "8042"))
KAFKA_RETRY_SECONDS = float(os.getenv("KAFKA_RETRY_SECONDS", "10"))
HEARTBEAT_INTERVAL_SECONDS = 15
PRODUCER_LOG_PROGRESS = os.getenv("PRODUCER_LOG_PROGRESS", "false").lower() in (
    "1",
    "true",
    "yes",
)

S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY = os.getenv("S3_KEY", Path(DEFAULT_MEASUREMENTS_FILE).name)
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
S3_CHUNK_SIZE = int(os.getenv("S3_CHUNK_SIZE_MB", "64")) * 1024 * 1024

submission_speed = DEFAULT_SUBMISSION_SPEED
sent_bytes = 0
speed_lock = threading.Lock()
stats_lock = threading.Lock()


def reverse_readlines(path, start_pos=0, chunk_size=1024 * 1024):
    with path.open("rb") as f:
        f.seek(0, 2)
        buffer = b""
        position = f.tell()

        while position > start_pos:
            read_size = (
                chunk_size
                if position - start_pos >= chunk_size
                else position - start_pos
            )
            position -= read_size
            f.seek(position)
            data = f.read(read_size)
            buffer = data + buffer
            *lines, buffer = buffer.split(b"\n")
            for line in reversed(lines):
                if line:
                    yield line.decode("utf-8")

        if buffer:
            yield buffer.decode("utf-8")


def parse_brokers(brokers):
    return [broker.strip() for broker in brokers.split(",") if broker.strip()]


def get_s3_client():
    if boto3 is None:
        raise SystemExit(
            "[ERROR] S3_BUCKET was set, but boto3 is not installed. "
            "Install boto3 or unset S3_BUCKET to use the local CSV file."
        )
    return boto3.client("s3", region_name=AWS_REGION)


def get_s3_header_and_start_pos(s3):
    response = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    object_size = response["ContentLength"]
    if object_size == 0:
        return "", 0, 0

    read_size = min(S3_CHUNK_SIZE, object_size)
    start = 0
    while start < object_size:
        end = min(object_size - 1, start + read_size - 1)
        response = s3.get_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Range=f"bytes={start}-{end}",
        )
        data = response["Body"].read()
        newline_index = data.find(b"\n")
        if newline_index >= 0:
            header = data[:newline_index].decode("utf-8")
            return header, start + newline_index + 1, object_size
        start = end + 1

    return data.decode("utf-8"), object_size, object_size


def reverse_s3_readlines(s3, start_pos, object_size):
    buffer = b""
    position = object_size

    while position > start_pos:
        read_size = min(S3_CHUNK_SIZE, position - start_pos)
        chunk_start = position - read_size
        chunk_end = position - 1
        response = s3.get_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Range=f"bytes={chunk_start}-{chunk_end}",
        )
        data = response["Body"].read()
        buffer = data + buffer
        *lines, buffer = buffer.split(b"\n")
        for line in reversed(lines):
            if line:
                yield line.decode("utf-8")
        position = chunk_start

    if buffer:
        yield buffer.decode("utf-8")


def get_csv_reader():
    if not S3_BUCKET:
        csv_path = CSV_PATH
        if PRODUCER_LOG_PROGRESS:
            print("Output format of first 5 rows: ", flush=True)
            with csv_path.open(encoding="utf-8") as f:
                for i in range(5):
                    print(f.readline().strip(), flush=True)

        with csv_path.open("rb") as csvfile:
            header_bytes = csvfile.readline()
            start_pos = csvfile.tell()

        header = header_bytes.decode("utf-8")
        reversed_lines = (
            line
            for line in reverse_readlines(csv_path, start_pos=start_pos)
            if line.strip()
        )
        return header, csv.DictReader(itertools.chain([header], reversed_lines))

    s3 = get_s3_client()
    print(
        f"[INFO] Streaming measurements from s3://{S3_BUCKET}/{S3_KEY} "
        f"in {S3_CHUNK_SIZE // 1024 // 1024} MB chunks...",
        flush=True,
    )
    header, start_pos, object_size = get_s3_header_and_start_pos(s3)
    reversed_lines = (
        line
        for line in reverse_s3_readlines(s3, start_pos, object_size)
        if line.strip()
    )
    return header, csv.DictReader(itertools.chain([header], reversed_lines))


def get_submission_speed():
    with speed_lock:
        return submission_speed


def set_submission_speed(value):
    global submission_speed
    with speed_lock:
        submission_speed = value
        return submission_speed


def add_sent_bytes(byte_count):
    global sent_bytes
    with stats_lock:
        sent_bytes += byte_count
        return sent_bytes


def get_producer_settings():
    with speed_lock:
        speed = submission_speed
    with stats_lock:
        bytes_sent = sent_bytes
    return {
        "submission_speed": speed,
        "sent_bytes": bytes_sent,
        "sent_mb": bytes_sent / 1024 / 1024,
    }


def create_kafka_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version_auto_timeout_ms=10_000,
                max_block_ms=10_000,
            )
        except Exception as error:
            print(
                f"[WARN] Cannot connect to Kafka at {KAFKA_BROKER}: {error}. "
                f"Retrying in {KAFKA_RETRY_SECONDS:g}s...",
                flush=True,
            )
            time.sleep(KAFKA_RETRY_SECONDS)


def main():
    if get_submission_speed() < 0 or get_submission_speed() > 100_000_000:
        raise SystemExit("[ERROR] SUBMISSION_SPEED must be between 0 and 100 000 000.")

    header, reader = get_csv_reader()
    if not header:
        print("[WARN] CSV file is empty.", flush=True)
        return

    print(
        f"[INFO] Starting Producer. Sending data to topic '{TOPIC}' on broker '{KAFKA_BROKER}'...",
        flush=True,
    )
    producer = create_kafka_producer()
    count = 0
    last_heartbeat = time.monotonic()

    rate_window_start = time.monotonic()
    rate_window_count = 0
    rate_last_speed = None

    def reset_rate_window():
        nonlocal rate_window_start, rate_window_count, rate_last_speed
        rate_window_start = time.monotonic()
        rate_window_count = 0
        rate_last_speed = None

    try:
        for row in reader:
            speed = get_submission_speed()
            while speed <= 0:
                time.sleep(0.1)
                speed = get_submission_speed()
                reset_rate_window()

            if speed != rate_last_speed:
                reset_rate_window()
                rate_last_speed = speed

            payload_size = len(json.dumps(row).encode("utf-8"))
            while True:
                try:
                    producer.send(TOPIC, value=row)
                    add_sent_bytes(payload_size)
                    break
                except Exception as error:
                    print(
                        f"[WARN] Could not send to Kafka topic '{TOPIC}': {error}. "
                        f"Retrying in {KAFKA_RETRY_SECONDS:g}s...",
                        flush=True,
                    )
                    try:
                        producer.close()
                    except Exception:
                        pass
                    time.sleep(KAFKA_RETRY_SECONDS)
                    producer = create_kafka_producer()
                    reset_rate_window()

            count += 1
            rate_window_count += 1

            now = time.monotonic()
            if now - last_heartbeat >= HEARTBEAT_INTERVAL_SECONDS:
                print(f"[INFO] Producer active: {count} messages sent", flush=True)
                last_heartbeat = now

            # Sleep only when ahead of target rate.
            # Threshold of 0.5ms avoids wasting syscalls on tiny sleeps.
            target_elapsed = rate_window_count / speed
            actual_elapsed = now - rate_window_start
            sleep_time = target_elapsed - actual_elapsed
            if sleep_time > 0.0005:
                time.sleep(sleep_time)

            # Reset window every 10 000 messages to keep float arithmetic clean.
            if rate_window_count >= 10_000:
                reset_rate_window()
                rate_last_speed = speed

        producer.flush()
    finally:
        producer.close()


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


@app.route("/producer", methods=["GET", "POST", "DELETE", "OPTIONS"])
def adjustProducerSpeed():
    if request.method == "OPTIONS":
        return "", 204

    if request.method == "GET":
        return jsonify(get_producer_settings())

    if request.method == "POST":
        input_value = request.args.get("speed", request.args.get("input"))

        if input_value is None:
            body = request.get_json(silent=True) or {}
            input_value = body.get("speed", body.get("input"))

        if input_value is None:
            input_value = request.form.get("speed", request.form.get("input"))

        if input_value is None:
            return (
                "Missing producer speed. Send JSON {'speed': value} or query parameter 'input'.",
                400,
            )

        try:
            requested_speed = float(input_value)
        except (TypeError, ValueError):
            return "Producer speed must be a number", 400

        if requested_speed < 0 or requested_speed > 100_000_000:
            return "Producer speed must be between 0 and 100_000_000", 400

        set_submission_speed(requested_speed)
        return jsonify(get_producer_settings())

    set_submission_speed(DEFAULT_SUBMISSION_SPEED)
    return jsonify(get_producer_settings())


if __name__ == "__main__":
    main()
