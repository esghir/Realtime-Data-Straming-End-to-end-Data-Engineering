import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster

# --- Fonctions dial Cassandra ---
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("Table created successfully!")

# --- Fonctions dial Spark ---
def create_spark_connection():
    s_conn = None
    try:
        packages = (
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1"
        )
        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.jars.packages", packages) \
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully.")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    return sel

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn:
        spark_df = connect_to_kafka(spark_conn)
        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = Cluster(['localhost']).connect()
            if session:
                create_keyspace(session)
                create_table(session)
                logging.info("Streaming is starting...")
                streaming_query = (selection_df.writeStream
                    .format("org.apache.spark.sql.cassandra")
                    .option('checkpointLocation', '/tmp/checkpoint')
                    .option("keyspace", "spark_streams")
                    .option("table", "created_users")
                    .start())
                streaming_query.awaitTermination()