"""
Script de vérification : relit les fichiers Parquet/JSON exportés
par consume_tickets.py et affiche un aperçu, pour confirmer l'export.

Usage : uv run verify_exports.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyExports").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Tickets détaillés (Parquet) ===")
detail_df = spark.read.parquet("output/tickets_detail/parquet")
print(f"Nombre de lignes : {detail_df.count()}")
detail_df.show(5, truncate=False)

print("\n=== Agrégation par type (JSON) ===")
agg_df = spark.read.json("output/tickets_by_type/json")
print(f"Nombre de lignes : {agg_df.count()}")
agg_df.show(20, truncate=False)

spark.stop()