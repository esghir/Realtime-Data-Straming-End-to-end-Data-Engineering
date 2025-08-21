import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
# L'ISLA7: Zedt les imports li kano naqssin
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster

# --- Fonctions dial Cassandra ---
def create_keyspace(session):
    """Creates the keyspace in Cassandra if it doesn't exist."""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    """Creates the user data table in Cassandra if it doesn't exist."""
    # L'ISLA7: Zedt l'colonne 'dob' li kant naqsa
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
    """Creates and configures the Spark Session."""
    s_conn = None
    try:
        # L'ISLA7: Bedelna les versions dial les packages l _2.13
        packages = (
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1"
        )

        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.jars.packages", packages) \
            .config("spark.cassandra.connection.host", "cassandra") \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully.")
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
    
    return s_conn

def connect_to_kafka(spark_conn):
    """Reads the data stream from the Kafka topic."""
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    """Parses the JSON data from the Kafka stream into a structured DataFrame."""
    # L'ISLA7: Zedt l'colonne 'dob' l schema
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
    # 1. Create Spark Connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # 2. Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        
        if spark_df:
            # 3. Parse Kafka data
            selection_df = create_selection_df_from_kafka(spark_df)
            
            # 4. Setup Cassandra
            session = Cluster(['localhost']).connect()
            
            if session:
                create_keyspace(session)
                create_table(session)
                
                # 5. Start streaming to Cassandra
                logging.info("Streaming is starting...")
                
                streaming_query = (selection_df.writeStream
                    .format("org.apache.spark.sql.cassandra")
                    .option('checkpointLocation', '/tmp/checkpoint')
                    .option("keyspace", "spark_streams")
                    .option("table", "created_users")
                    .start())
                
                # L'ISLA7: S7e7t l'ghalat l'imla2i
                streaming_query.awaitTermination()

        else:
            logging.error("Failed to create Cassandra session.")


