# pyrefly: ignore[missing-import]
from pyflink.common import Types
# pyrefly: ignore[missing-import]
from pyflink.datastream import StreamExecutionEnvironment


def run_stateful_experiment():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("--- Starting Stateful Aggregation Stream ---")

    # Simulated continuous stream of sensor readings with varying values
    readings = [
        ("sensor_1", 20.0),
        ("sensor_2", 35.0),
        ("sensor_1", 28.5),  # sensor_1 max should update to 28.5
        ("sensor_2", 31.0),  # sensor_2 max stays 35.0
        ("sensor_1", 25.0),  # sensor_1 max stays 28.5
        ("sensor_2", 42.0),  # sensor_2 max should update to 42.0
        ("sensor_1", 33.2),  # sensor_1 max should update to 33.2
    ]

    stream = env.from_collection(
        collection=readings,
        type_info=Types.ROW_NAMED(
            ["sensor_id", "temperature"], [Types.STRING(), Types.FLOAT()]
        ),
    )

    # 1. key_by: separates the stream into independent state partitions per sensor_id
    keyed_stream = stream.key_by(lambda row: row.sensor_id)

    # 2. reduce: compares current max with the new incoming reading and keeps the highest
    rolling_max_stream = keyed_stream.reduce(
        lambda prev, curr: (
            prev if prev.temperature >= curr.temperature else curr
        )
    )

    # 3. Format and print
    rolling_max_stream.map(
        lambda row: f"[STATE] Sensor: {row.sensor_id} | Current Peak Temp: {row.temperature}°C",
        output_type=Types.STRING(),
    ).print()

    env.execute("Stateful Rolling Peak Temperature Job")


if __name__ == "__main__":
    run_stateful_experiment()
