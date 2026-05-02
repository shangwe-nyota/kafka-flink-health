import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)


def build_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # For this hello-world we keep it simple and use processing time only.
    return env


def main():
    env = build_env()

    # Kafka source: read JSON strings from hello_input
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("hello_input")
        .set_group_id("flink-hello-world")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Simple pipeline:
    #   raw JSON string -> dict -> add field -> JSON string
    raw_stream = env.from_source(
        source,
        # PyFlink expects an explicit watermark strategy here on this local setup.
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="hello_input_source",
    )

    def parse_and_annotate(s: str) -> str:
        try:
            obj = json.loads(s)
        except json.JSONDecodeError:
            obj = {"raw": s}
        obj["processed_by"] = "hello_flink_job"
        return json.dumps(obj)

    processed = raw_stream.map(
        parse_and_annotate,
        output_type=Types.STRING(),
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("hello_output")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    processed.sink_to(sink)

    env.execute("HelloFlinkJob")


if __name__ == "__main__":
    main()
