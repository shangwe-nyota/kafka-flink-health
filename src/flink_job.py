import json
from datetime import datetime
from typing import Any, Dict
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner

from pyflink.datastream import (
    StreamExecutionEnvironment,
    TimeCharacteristic,
)
from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.time import Time
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema


def parse_event(value: str) -> Dict[str, Any]:
    """
    Parse a JSON string into a dict and perform basic validation.

    This function is pure Python and will be unit-tested.
    It should:
      - parse JSON
      - ensure required fields exist: patient_id, timestamp, heart_rate_bpm
      - convert timestamp to a Python datetime or epoch millis
      - possibly clamp heart_rate_bpm to a valid range or drop invalid records

    TODO: implement parsing & validation logic.
    """
    try:
        obj = json.loads(value)

        # check required fields
        if "patient_id" not in obj or "timestamp" not in obj or "heart_rate_bpm" not in obj:
            return None

        patient_id = obj["patient_id"]
        timestamp = obj["timestamp"]
        heart_rate_bpm = obj["heart_rate_bpm"]

        # validate types
        if not isinstance(patient_id, str):
            return None
        if not isinstance(heart_rate_bpm, int):
            return None

        # parse timestamp → epoch millis
        timestamp = timestamp.replace("Z", "+00:00")
        dt = datetime.fromisoformat(timestamp)
        event_time = int(dt.timestamp() * 1000)

        return {
            "patient_id": patient_id,
            "heart_rate_bpm": heart_rate_bpm,
            "event_time": event_time,
        }

    except Exception:
        return None


def classify_window(avg_hr: float) -> str:
    """
    Classify a window based on its average heart rate.

    Returns one of: "tachycardia", "bradycardia", "normal".

    TODO: implement classification logic using thresholds, e.g.:
      - avg_hr > 100 -> "tachycardia"
      - avg_hr < 50  -> "bradycardia"
      - else -> "normal"
    """
    if avg_hr > 100:
        return "tachycardia"
    elif avg_hr < 50:
        return "bradycardia"
    else:
        return "normal"

def build_env() -> StreamExecutionEnvironment:
    """
    Create and configure the StreamExecutionEnvironment.

    This is factored out for easier testing.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(5000)  # 5s, can be tuned
    return env
class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value["event_time"])

class HeartRateWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        readings = list(elements)

        if not readings:
            return

        heart_rates = [int(e["heart_rate_bpm"]) for e in readings]

        avg_hr = sum(heart_rates) / len(heart_rates)
        min_hr = min(heart_rates)
        max_hr = max(heart_rates)

        alert = {
            "patient_id": key,
            "window_start": context.window().start,
            "window_end": context.window().end,
            "avg_hr": round(avg_hr, 2),
            "min_hr": min_hr,
            "max_hr": max_hr,
            "alert_type": classify_window(avg_hr),
        }

        yield json.dumps(alert)
def main():
    env = build_env()

    # Define Kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("heart_rate_events")
        .set_group_id("flink-heart-monitor")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Ingest raw strings first. We assign event-time after parsing because
    # the raw Kafka value is still just a JSON string at this point.
    ds = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "heart_rate_events_source",
    )

    # Parse events
    parsed = ds.map(
        lambda s: parse_event(s),
        output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()),
    )

    # Filter out invalid records
    parsed = parsed.filter(lambda e: e is not None)

    # Assign timestamps/watermarks based on parsed event-time.
    watermarked = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(EventTimeAssigner())
    )
    alerts_stream = (
        watermarked
        .key_by(lambda e: e["patient_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(HeartRateWindowFunction(), output_type=Types.STRING())
    )

    # Kafka sink for alerts
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("heart_rate_alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    alerts_stream.sink_to(sink)

    env.execute("HeartRateAlertsJob")


if __name__ == "__main__":
    main()
