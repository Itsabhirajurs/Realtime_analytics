# pyrefly: ignore[missing-import]
from pyflink.common import Types
# pyrefly: ignore[missing-import]
from pyflink.datastream import StreamExecutionEnvironment


def run_experiment():
    # 1. Initialize the Flink Streaming Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Set parallelism to 1 so the output prints cleanly in order in your terminal
    env.set_parallelism(1)

    print("--- Starting PyFlink Stream Experiment ---")

    # 2. Create a mock streaming source (simulating real-time sensor / event data)
    sensor_readings = [
        ("sensor_1", 22.5),
        ("sensor_2", 45.1),
        ("sensor_1", 24.0),
        ("sensor_3", 85.3),  # High temperature alert!
        ("sensor_2", 44.8),
        ("sensor_3", 91.0),  # High temperature alert!
    ]

    # Ingest the collection into a DataStream
    data_stream = env.from_collection(
        collection=sensor_readings,
        type_info=Types.ROW_NAMED(
            ["sensor_id", "temperature"], [Types.STRING(), Types.FLOAT()]
        ),
    )

    # 3. Stream Transformations
    # A. Filter: only keep high temperatures (> 40.0)
    high_temp_stream = data_stream.filter(lambda row: row.temperature > 40.0)

    # B. Map: format into an alert message
    alert_stream = high_temp_stream.map(
        lambda row: f"[ALERT] High temperature on {row.sensor_id}: {row.temperature}°C",
        output_type=Types.STRING(),
    )

    # 4. Sink: print each event as it is processed in real time
    alert_stream.print()

    # 5. Execute the pipeline
    env.execute("Sensor Alert Stream Job")


if __name__ == "__main__":
    run_experiment()
