from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class SparkEngine():
    def __init__(self, session: SparkSession):

        self.session = session
        

    def search(self, urls: list[str])-> DataFrame:

        return self.session.read.parquet(*urls)