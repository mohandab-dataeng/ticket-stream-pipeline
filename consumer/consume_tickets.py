"""
Consumer PySpark pour le topic Redpanda `client_tickets`.

Lit le flux de tickets en streaming (Spark Structured Streaming), applique
des transformations (mapping équipe support selon le type de demande) et
des agrégations (nombre de tickets par type, sur une fenêtre glissante),
puis affiche les résultats en continu dans la console.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, window, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC_NAME = "client_tickets"
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

TEAM_MAPPING = {
    "Support technique": "Équipe Tech",
    "Facturation": "Équipe Facturation",
    "Livraison": "Équipe Logistique",
    "Réclamation": "Équipe Qualité",
    "Information": "Équipe Accueil",
}

TICKET_SCHEMA = StructType([
    StructField("ticket_id", StringType()),
    StructField("client_id", StringType()),
    StructField("created_at", StringType()),
    StructField("request", StringType()),
    StructField("request_type", StringType()),
    StructField("priority", StringType()),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TicketStreamConsumer")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_ticket_stream(spark: SparkSession):
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    return raw_stream.select(
        from_json(col("value").cast("string"), TICKET_SCHEMA).alias("ticket"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("ticket.*", "kafka_timestamp")


def apply_transformations(tickets_df):
    team_column = None
    for request_type, team in TEAM_MAPPING.items():
        team_column = when(col("request_type") == request_type, team) if team_column is None \
            else team_column.when(col("request_type") == request_type, team)
    team_column = team_column.otherwise("Équipe Générale")

    return tickets_df.withColumn("assigned_team", team_column)


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    tickets = read_ticket_stream(spark)
    enriched = apply_transformations(tickets)

    enriched.writeStream.format("console").outputMode("append") \
        .option("truncate", "false").queryName("tickets_detail_console").start()

    enriched.writeStream.format("parquet").outputMode("append") \
        .option("path", "output/tickets_detail/parquet") \
        .option("checkpointLocation", "output/tickets_detail/_checkpoint") \
        .queryName("tickets_detail_parquet").start()

    counts_by_type = (
        enriched
        .withColumn("event_time", col("kafka_timestamp").cast(TimestampType()))
        .withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), "30 seconds"), col("request_type"))
        .agg(count("*").alias("ticket_count"))
    )

    counts_by_type.writeStream.format("console").outputMode("update") \
        .option("truncate", "false").queryName("tickets_by_type_console").start()

    counts_by_type.writeStream.format("json").outputMode("append") \
        .option("path", "output/tickets_by_type/json") \
        .option("checkpointLocation", "output/tickets_by_type/_checkpoint") \
        .queryName("tickets_by_type_json").start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()