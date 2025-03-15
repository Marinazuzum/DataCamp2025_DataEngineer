from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.window import Session
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.table.expressions import col, lit

def create_trips_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_watermark AS lpep_dropoff_datetime,
            WATERMARK FOR event_watermark AS event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def create_trips_sink_postgres(t_env):
    table_name = "trips_aggregated"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            trip_streak BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def longest_unbroken_streak():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    #env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_trips_source_kafka(t_env)
        sink_table = create_trips_sink_postgres(t_env)

        table = t_env.from_path(source_table)

        # Create a session window with a gap of 5 minutes
        aggregated = table.window(Session.with_gap(lit(5 * 60).seconds).on(col("event_watermark")).alias("w")) \
            .group_by(
                col('w'),
                col('PULocationID'),
                col('DOLocationID')
            ) \
            .select(
                col("w").start.alias("window_start"), 
                col("w").end.alias("window_end"),
                col('PULocationID'),
                col('DOLocationID'),
                col('PULocationID').count.alias('trip_streak')
            )


        # Insert the result into the sink
        aggregated.execute_insert(sink_table).wait()

        # t_env.execute_sql(f"""
        # INSERT INTO {sink_table}
        # SELECT
        #     event_watermark as window_start,
        #     event_watermark as window_end,
        #     PULocationID,
        #     DOLocationID,
        #     1 as trip_streak
        # FROM {source_table}
        # """).wait()

        print("Executing query to determine longest unbroken taxi trip streak and sinking results to PostgreSQL...")
    
    except Exception as e:
        print("Processing Kafka stream failed:", str(e))

if __name__ == '__main__':
    longest_unbroken_streak()
