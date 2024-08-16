from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Criação da sessão do Spark
spark = SparkSession.builder \
    .appName("Ingestão de Dados Batch") \
    .config("spark.sql.warehouse.dir", "hdfs://127.0.0.1:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

input_ft = "hdfs://127.0.0.1:9000/bronze/ft_custo_de_vida_por_pais.csv"
input_dim = "hdfs://127.0.0.1:9000/bronze/dim_pessoas.csv"


#Leitura dos arquivos CSV da camada Bronze
df_custo_vida = spark.read.csv(
    input_ft, 
    header=True,
    inferSchema=True)

print("Estrutura df_custo_vida: ")
df_custo_vida.printSchema()


df_pessoas = spark.read.csv(
    input_dim,
    header=True,
    inferSchema=True)

print("Estrutura df_pessoas: ")
df_pessoas.printSchema()

df_custo_vida.show(5)
df_pessoas.show(5)

#Otimização 1: Reparticionamento
df_custo_vida = df_custo_vida.repartition(2)
df_pessoas = df_pessoas.repartition(2)

#Otimização 2: Cache para acelerar operações subsequentes
df_custo_vida.cache()
df_custo_vida.count()
df_pessoas.cache()
df_pessoas.count()

#Otimização 3: Join entre os datafremes e ordenacao para otimizar posterior leitura
df_joined = df_custo_vida.join(df_pessoas, on="Country Cod", how="inner")

df_joined.show(5)

df_joined.write.mode("overwrite").parquet("hdfs://127.0.0.1:9000/silver/joined_data")

df_joined_sorted = df_joined.orderBy("Country Cod", "Rank")
df_joined_sorted.show(5)

#Otimização 4: Particionamento
df_joined_sorted.write \
    .mode("overwrite") \
    .partitionBy("Year") \
    .parquet("hdfs://127.0.0.1:9000/gold/joined_data_optimized")

#Escrita no Hive Warehouse
df_joined.write.mode("overwrite").saveAsTable("default.joined_data_hive")
df_custo_vida.write.mode("overwrite").saveAsTable("default.fat_custo_vida")
df_pessoas.write.mode("overwrite").saveAsTable("default.dim_pessoas")

#Verificando no Hive Warehouse
tables = spark.sql("SHOW TABLES IN default")
tables.show()

#Estrutura e conteudo da tabela
spark.sql("DESCRIBE default.joined_data_hive").show()

df_query = spark.sql("SELECT * FROM default.joined_data_hive")
df_query.show()

#Consulta no Hive Warehouse
df_query_result = spark.sql("""
                            SELECT Country as pais
                            , count(1) as qtd
                            , avg(Salary) as media_salarial
                            FROM default.joined_data_hive
                            WHERE Year = 2024
                            AND Rank >= 5
                            GROUP BY Country
                            ORDER BY avg(Salary) desc
                            """)

df_query_result.show()

df_query_result.write.mode("overwrite").parquet("hdfs://127.0.0.1:9000/gold/mediaSalarialByCountry")

#Otimização 5: Otimização de JOIN (Broadcast ou Partitioned Join)
#Broadcast Join (Caso de dados pequenos)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  #Desativando broadcast automático
df_broadcast_joined = df_custo_vida.join(F.broadcast(df_pessoas), on="Country Cod", how="inner")

#Partitioned Join (Caso de dados grandes)
df_partitioned = df_custo_vida.repartition("Country Cod")
df_partitioned_joined = df_partitioned.join(df_pessoas, on="Country Cod", how="inner")

# Encerramento da sessão do Spark
spark.stop()
