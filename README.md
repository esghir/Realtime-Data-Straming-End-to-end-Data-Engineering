# Real-Time User Data Streaming & Analytics Pipeline

This project demonstrates a complete, end-to-end, real-time data engineering pipeline.  
It is designed to ingest a continuous stream of user data from a public API, process it in real-time, and store it in a scalable NoSQL database.  
The entire infrastructure is containerized using **Docker Compose**, making it portable and easy to deploy.

This pipeline showcases the integration of several industry-standard technologies to handle high-throughput data streams, making it an excellent example of a **modern data engineering architecture**.

---

## 🏛️ System Architecture

The pipeline is built around a **distributed, event-driven architecture**.  
Data flows from the source through a messaging queue, is processed by a distributed computing engine, and is finally stored in a distributed database.

### Data Flow Diagram
This diagram illustrates the journey of data from the initial API call to its final destination in Cassandra.

```mermaid
graph TD
    subgraph "Data Source & Orchestration"
        A["Random User API"] -- "1. Fetch Raw User Data" --> B["Airflow DAG<br/>(kafka_stream.py)"];
    end

    subgraph "Streaming Platform (Kafka Ecosystem)"
        B -- "2. Produce JSON Messages" --> C["<b>Kafka Topic</b><br/>(users_created)"];
        ZK["Zookeeper"] -- "Manages" --> K["Kafka Broker"];
    end
    
    subgraph "Real-Time Processing (Spark Cluster)"
        C -- "3. Consume Stream" --> D["<b>PySpark Application</b><br/>(spark_stream.py)"];
        SM["Spark Master"] -- "Coordinates" --> SW["Spark Worker"];
    end

    subgraph "Data Storage (NoSQL Database)"
        D -- "4. Write Processed Data" --> E["<b>Cassandra DB</b><br/>(keyspace: spark_streams)"];
    end
