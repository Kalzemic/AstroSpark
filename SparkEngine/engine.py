from pyspark.sql import SparkSession
from queryparser.adql import ADQLQueryTranslator
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType
from utils.regex import fix_distance, fix_contains
from functools import reduce

class SparkEngine():
    def __init__(self, session: SparkSession):
        self.session = session
        self.partition_cache = {}  

    def _load_partitions(self, urls: list[str]):
        for url in urls:
            if url not in self.partition_cache:
                df = self.session.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("comment", "#") \
                    .option("nullValue", "") \
                    .csv(url)

                exprs = [
                    expr(f"TRY_CAST(`{f.name}` AS DOUBLE)").alias(f.name) if isinstance(f.dataType, StringType) else col(f.name)
                    for f in df.schema.fields
                ]
                df = df.select(*exprs)
                df.cache()
                self.partition_cache[url] = df

    def search(self, adt: ADQLQueryTranslator, urls: list[str]):
        
        try:
            self._load_partitions(urls)

            relevant = [self.partition_cache[url] for url in urls]
            union_df = reduce(lambda a, b: a.union(b), relevant)
            union_df.createOrReplaceTempView("gaia_source")

            sql_query = adt.to_postgresql()
            sql_query = sql_query.replace('gaiadr3.gaia_source', 'gaia_source')
            sql_query = fix_distance(sql_query)
            sql_query = fix_contains(sql_query)
            self.session.sql(sql_query).show()
        except: 
            print('partition does not exist in the database')