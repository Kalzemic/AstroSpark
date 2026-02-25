from pyspark.sql import SparkSession
from confluent_kafka import Consumer
from typing import Dict
from optimizer.QueryOptimizer import QueryOptimizer
from queryparser.adql import ADQLQueryTranslator
from queryparser.adql import ADQLQueryTranslator
from queryparser.postgresql import PostgreSQLQueryProcessor
from SparkEngine.engine import SparkEngine


class AsteroideEngine():
    
    def __init__(self, kafka_config: Dict[str,str], topic: str, session: SparkSession):
        
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([topic])

        self.engine = SparkEngine(session)
        self.optimizer = QueryOptimizer()
    

    def process(self, raw_query: str):

        adt = ADQLQueryTranslator()

        adt.set_query(raw_query)
        adt.parse()
        print(adt.query)
        print(type(adt.tree))
        print(adt.tree.toStringTree(recog=adt.parser))
        
        urls = self.optimizer(adt)

        # dataframe = self.session.read.parquet(*urls)

        # dataframe.show()

    
    def run(self):

        while True:
            message = self.consumer.poll(timeout=1.0)
        
            if message is None:
                continue
            if message.error():
                print(f"Consumer error: {message.error()}")
                continue
            
            self.process(message.value().decode('utf-8'))


        

