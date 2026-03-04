from pyspark.sql import SparkSession
from queryparser.adql import ADQLQueryTranslator
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType
from utils.regex import fix_distance
def main():

    print("enter query here:")
    query = input() 
    adt = ADQLQueryTranslator(query)

    adt.parse()
    sql_query =adt.to_postgresql()
    sql_query = sql_query.replace("gaiadr3.gaia_source", "gaia_source")
    sql_query = fix_distance(sql_query)

    spark = SparkSession.builder.appName('Asteroide-Test').getOrCreate()

    df = spark.read.option("header","true")\
        .option("inferSchema","true")\
            .option("comment","#")\
            .option("nullValue", "")\
            .csv("data/GaiaSource_000000-003111.csv.gz")

    

    exprs = [
        expr(f"TRY_CAST(`{f.name}` AS DOUBLE)").alias(f.name) if isinstance(f.dataType, StringType) else col(f.name)
        for f in df.schema.fields
    ]

    df = df.select(*exprs)
    df.createOrReplaceTempView("gaia_source")
    result = spark.sql(sql_query)
    result.show()


if __name__ == '__main__':
    main()