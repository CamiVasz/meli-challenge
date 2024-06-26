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
    # Create a SparkSession
    spark = SparkSession.builder.appName("descubri_mas").getOrCreate()
    # Read the JSON files
    prints = read_json("prints.json")
    prints = prints.select(
        col("day"),
        col("event_data.position").alias("position"),
        col("event_data.value_prop").alias("value_prop"),
        col("user_id"),
    )
    prints.show()
    taps = read_json("taps.json")
    taps = taps.select(
        col("day"),
        col("event_data.position").alias("position"),
        col("event_data.value_prop").alias("value_prop"),
        col("user_id"),
    ).selectExpr(
        "day as tap_day",
        "position as tap_position",
        "value_prop as tap_value_prop",
        "user_id as tap_user_id",
    )
    # read the csv file
    payments = spark.read.csv("pays.csv", header=True)
    payments = payments.selectExpr(
        "pay_date",
        "total",
        "user_id as pay_user_id",
        "value_prop as pay_value_prop",
    )

if __name__=='__main__':
    main()
