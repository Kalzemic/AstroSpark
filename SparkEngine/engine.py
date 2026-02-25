from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from queryparser.adql import ADQLQueryTranslator

class SparkEngine():
    def __init__(self, session: SparkSession):

        self.session = session
        

    def search(self, adt:ADQLQueryTranslator, urls: list[str]):

        df = self.session.read.parquet(*urls)
        sql_query = adt.to_postgresql()

        df.createOrReplaceTempView("gaiadr3.gaia_source")
        result_df = self.session.sql(sql_query)
        result_df.show()

