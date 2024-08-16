# bigdata
### Desafio - Solução Big Data

##### Processo de Ingestão Utilizando Ecosistema Hadoop

1) app_batch.py: Aplicação que simula ingestão utilzando os serviços Hadoop(HDFS), Spark e Hive.


2) app_real_time.py: Aplicação que simula ingestão em tempo real, utilizando serviços Hadoop(HDFS), Spark, Hive e Kafka. 


3) data_producer_real_time.py: Aplicação que gera dados para o tópico criado no Kafka. (Deve ser iniciado antes da aplicação app_real_time.py, pois irá gerar dados em tempo teal, para captura e posterior ingestão no HDFS).