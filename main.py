from confluent_kafka import Consumer
from pyspark.sql import SparkSession
from AsteroideEngine.engine import AsteroideEngine




if __name__ == "__main__":
    spark = SparkSession.builder.appName('Asteroide').getOrCreate()
    kafka_config = {'bootstrap.servers':'localhost:9092',"group.id": "astro-query-group","auto.offset.reset": "earliest"}
    engine = AsteroideEngine(kafka_config=kafka_config,topic='adql-queries',session=spark)
    engine.run()