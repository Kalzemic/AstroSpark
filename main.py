from pyspark.sql import SparkSession
from AsteroideEngine.engine import AsteroideEngine




if __name__ == "__main__":
    spark = SparkSession.builder.appName('Asteroide') \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .getOrCreate()
    kafka_config = {'bootstrap.servers':'localhost:9092',
                    "group.id": "astro-query-group",
                    "auto.offset.reset": "earliest",
                    "max.poll.interval.ms": "3600000"}
    engine = AsteroideEngine(kafka_config=kafka_config,topic='adql-queries',session=spark)
    engine.run()