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

# --- Configuration ---
BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC_NAME = "client_tickets"

# Doit correspondre EXACTEMENT à la version de PySpark installée (4.2.0)
# et au suffixe Scala de cette version (2.13 depuis Spark 4.0+).
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

# --- Mapping type de demande -> équipe support assignée ---
# (transformation demandée par l'énoncé Step 2, "ex: ajoutez automatiquement
# le nom d'une équipe de support assignée en fonction du type de demande")
TEAM_MAPPING = {
    "Support technique": "Équipe Tech",
    "Facturation": "Équipe Facturation",
    "Livraison": "Équipe Logistique",
    "Réclamation": "Équipe Qualité",
    "Information": "Équipe Accueil",
}

# Schéma exact des tickets envoyés par produce_tickets.py.
# Obligatoire : Spark ne peut pas déduire le schéma d'un flux Kafka tout seul,
# la donnée y arrive toujours sous forme d'octets bruts (colonne "value").
TICKET_SCHEMA = StructType([
    StructField("ticket_id", StringType()),
    StructField("client_id", StringType()),
    StructField("created_at", StringType()),
    StructField("request", StringType()),
    StructField("request_type", StringType()),
    StructField("priority", StringType()),
])


def build_spark_session() -> SparkSession:
    """Crée la session Spark, avec le connecteur Kafka déclaré explicitement."""
    return (
        SparkSession.builder
        .appName("TicketStreamConsumer")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")  # petit cluster local, pas besoin de plus
        .getOrCreate()
    )


def read_ticket_stream(spark: SparkSession):
    """Ouvre le flux brut Kafka/Redpanda et parse le JSON selon TICKET_SCHEMA."""
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")  # relit aussi les tickets déjà envoyés
        .load()
    )

    # La colonne "value" arrive en binaire (bytes) : on la décode en string
    # JSON, puis on l'éclate selon le schéma défini plus haut.
    return raw_stream.select(
        from_json(col("value").cast("string"), TICKET_SCHEMA).alias("ticket"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("ticket.*", "kafka_timestamp")


def apply_transformations(tickets_df):
    """Ajoute la colonne équipe support, transformation demandée par l'énoncé."""
    team_column = None
    for request_type, team in TEAM_MAPPING.items():
        condition = when(col("request_type") == request_type, team)
        team_column = condition if team_column is None else team_column.when(
            col("request_type") == request_type, team
        )
    # otherwise() gère le cas d'un type de demande inconnu (sécurité)
    team_column = team_column.otherwise("Équipe Générale")

    return tickets_df.withColumn("assigned_team", team_column)


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # réduit le bruit des logs Spark

    tickets = read_ticket_stream(spark)
    enriched = apply_transformations(tickets)

    # --- Sortie 1 : flux détaillé, ticket par ticket, avec équipe assignée ---
    detail_console_query = (
        enriched.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .queryName("tickets_detail_console")
        .start()
    )

    # Export Parquet des tickets détaillés (Step 3).
    # checkpointLocation est obligatoire en streaming : Spark s'en sert pour
    # savoir où il en est si le job redémarre, et ne jamais ré-écrire deux
    # fois le même ticket.
    detail_parquet_query = (
        enriched.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "output/tickets_detail/parquet")
        .option("checkpointLocation", "output/tickets_detail/_checkpoint")
        .queryName("tickets_detail_parquet")
        .start()
    )

    # --- Sortie 2 : agrégation, nombre de tickets par type, fenêtre de 30s ---
    # (transformation demandée par l'énoncé : "calculez le nombre de tickets
    # par type"). Nécessite une colonne timestamp de type TimestampType, donc
    # on caste kafka_timestamp qui contient déjà l'heure de réception Kafka.
    counts_by_type = (
        enriched
        .withColumn("event_time", col("kafka_timestamp").cast(TimestampType()))
        .withWatermark("event_time", "1 minute")
        .groupBy(
            window(col("event_time"), "30 seconds"),
            col("request_type"),
        )
        .agg(count("*").alias("ticket_count"))
    )

    agg_console_query = (
        counts_by_type.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .queryName("tickets_by_type_console")
        .start()
    )

    # Export JSON de l'agrégation (Step 3).
    # Le sink fichier n'accepte pas outputMode("update") pour une agrégation
    # avec fenêtre : on passe en "append", qui n'écrit une fenêtre que
    # lorsqu'elle est définitivement close (watermark dépassé) — cohérent
    # avec un export destiné à une analyse a posteriori, pas au temps réel.
    agg_json_query = (
        counts_by_type.writeStream
        .format("json")
        .outputMode("append")
        .option("path", "output/tickets_by_type/json")
        .option("checkpointLocation", "output/tickets_by_type/_checkpoint")
        .queryName("tickets_by_type_json")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()