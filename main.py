'''
Use this script to produce the
parquet file with all the info described
in the coding exercise.
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, trunc, date_trunc, date_sub, coalesce, lit
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
import datetime as dt

def read_json(path):
    '''
    Read data from a JSON file using an user-defined schema.
    - path: path to the JSON file.
    '''
    schema = StructType(
        [
            StructField("day", DateType(), True),
            StructField(
                "event_data",
                StructType(
                    [
                        StructField("position", IntegerType(), True),
                        StructField("value_prop", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("user_id", IntegerType(), True),
        ]
    )
    data = spark.read.json(path, schema=schema)
    return data

def get_last_week(data):
    pass

def main():
    pass

if __name__=='__main__':
    main()
