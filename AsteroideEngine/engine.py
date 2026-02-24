from pyspark.sql import SparkSession
from confluent_kafka import Consumer
from typing import Dict
from optimizer.QueryOptimizer import QueryOptimizer
from queryparser.adql import ADQLQueryTranslator
from SparkEngine.engine import SparkEngine


class AsteroideEngine():
    
    def __init__(self, kafka_config: Dict[str,str], topic: str, session: SparkSession):
        
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([topic])
        self.engine = SparkEngine(session)
        self.optimizer = QueryOptimizer()
    

    def process(self, raw_query: str):

        query = ADQLQueryTranslator(raw_query)

        urls = self.optimizer(query)

        dataframe = self.session.read.parquet(*urls)

        dataframe.show()

    
    def run(self):

        for message in self.consumer:

            self.process(message.value.decode('utf-8'))


        

