from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Criação da sessão do Spark
spark = SparkSession.builder \
    .appName("Ingestão de Dados Streaming") \
    .config("spark.sql.warehouse.dir", "hdfs://127.0.0.1:9000/user/hive/warehouse") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .enableHiveSupport() \
    .getOrCreate()

# Schema dos dados JSON recebidos no Kafka
schema = StructType() \
    .add("person_cod", IntegerType()) \
    .add("country_cod", StringType()) \
    .add("name", IntegerType()) \
    .add("date_birth", StringType()) \
    .add("salary", DoubleType())

# Configuração do Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "real_time_data"

# Leitura dos dados do tópico Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Conversão dos dados do Kafka de binário para String
df_kafka_value = df_kafka.selectExpr("CAST(value AS STRING)")

# Conversão de JSON para colunas utilizando o schema definido
df_parsed = df_kafka_value.withColumn("jsonData", from_json(col("value"), schema)) \
    .select(col("jsonData.*"))

# Processamento adicional dos dados (opcional)
df_processed = df_parsed.withColumn("salary_usd", expr("salary * 5.8"))

# Escrita dos dados processados no HDFS (camada Silver)
hdfs_path = "hdfs://127.0.0.1:9000/silver/real_time_data"

df_query_hdfs = df_processed.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "hdfs://127.0.0.1:9000/silver/checkpoints/") \
    .option("path", hdfs_path) \
    .outputMode("append") \
    .start()

# # Escrita dos dados no Hive (camada Gold)
# df_query_hive = df_processed.writeStream \
#     .format("hive") \
#     .option("checkpointLocation", "hdfs://127.0.0.1:9000/gold/checkpoints/") \
#     .outputMode("append") \
#     .queryName("real_time_data_hive") \
#     .start("default.real_time_data_hive")

# Aguardar a execução
df_query_hdfs.awaitTermination()
# df_query_hive.awaitTermination()

# Encerrar sessão do Spark
spark.stop()
