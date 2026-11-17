from pathlib import Path
from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment


def run_kafka_flink_job():
    print("--- Starting Flink Real-Time Kafka Stream Consumer with Web UI ---")

    # 1. Initialize Flink Configuration with Web UI enabled
    config = Configuration()
    config.set_string("rest.port", "8081")
    config.set_string("rest.bind-port", "8081-8085")

    env_settings = (
        EnvironmentSettings.new_instance()
        .with_configuration(config)
        .in_streaming_mode()
        .build()
    )
    t_env = TableEnvironment.create(env_settings)

    # 2. Tell Flink where the Kafka Bridge JAR is located
    jar_url = (
        Path("flink-sql-connector-kafka-5.0.0-2.2.jar").resolve().as_uri()
    )
    t_env.get_config().set("pipeline.jars", jar_url)

    # 3. Define the Kafka Source Table
    t_env.execute_sql(
        """
        CREATE TABLE kafka_sensor_readings (
            `time` DOUBLE,
            `acc_x` DOUBLE,
            `acc_y` DOUBLE,
            `acc_z` DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sensor_data',
            'properties.bootstrap.servers' = '172.17.204.151:9092',
            'properties.group.id' = 'flink_realtime_group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    )

    # 4. Stream Query: Calculate acceleration magnitude
    result_table = t_env.sql_query(
        """
        SELECT 
            `time`,
            `acc_x`,
            `acc_y`,
            `acc_z`,
            ROUND(SQRT(`acc_x`*`acc_x` + `acc_y`*`acc_y` + `acc_z`*`acc_z`), 3) AS magnitude
        FROM kafka_sensor_readings
    """
    )

    print(
        "🚀 Flink Web Dashboard is live at: http://localhost:8081 (or 8082 if"
        " busy)"
    )
    print("Listening to Kafka topic 'sensor_data'...")
    result_table.execute().print()


if __name__ == "__main__":
    run_kafka_flink_job()
