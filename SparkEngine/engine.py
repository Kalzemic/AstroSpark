from pyspark.sql import SparkSession
from queryparser.adql import ADQLQueryTranslator
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType


class SparkEngine():
    def __init__(self, session: SparkSession):

        self.session = session
        self.loaded_partitions = []


    def _load_partitions(self, urls: list[str]):
        
        new_urls= [u for u in urls if u not in self.loaded_partitions]
        if not new_urls: 
            return 
        all_urls = self.loaded_partitions + new_urls
        df = self.session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("comment", "#") \
            .option("nullValue", "") \
            .csv(all_urls)

        exprs = [
            expr(f"TRY_CAST(`{f.name}` AS DOUBLE)").alias(f.name) if isinstance(f.dataType, StringType) else col(f.name)
            for f in df.schema.fields
        ]

        df = df.select(*exprs)
        df.createOrReplaceTempView("gaia_source")
        self.loaded_partitions = all_urls



    def search(self, adt:ADQLQueryTranslator, urls: list[str]):
        
        self._load_partitions(urls)

        sql_query = adt.to_postgresql()
        sql_query = sql_query.replace('"gaiadr3"."gaia_source"', 'gaia_source')
        result_df = self.session.sql(sql_query)
        result_df.show()

