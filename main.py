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

def get_last_week(prints):
    max_day = prints.agg({"day": "max"}).collect()[0]["max(day)"]
    # get last week from the max day
    last_week = max_day - dt.timedelta(days=7)
    prints_lw = prints[prints["day"] >= last_week]
    return prints_lw

def get_last_week_tapped(prints_lw, taps):
    lw_taps = (
        prints_lw.join(
            taps,
            (prints_lw["day"] == taps["tap_day"])
            & (prints["user_id"] == taps["tap_user_id"])
            & (prints["value_prop"] == taps["tap_value_prop"]),
            "left",
        )
        .withColumn("tap_last_week", col("tap_day").isNotNull().cast("int"))
        .groupBy(col("user_id"), col("value_prop"), col("day"))
        .max()
        .selectExpr(
            "day as lw_taps_day",
            "user_id as lw_taps_user_id",
            "value_prop as lw_taps_value_prop",
            "`max(tap_last_week)` as was_tapped",
        )
    )

def get_last_three_weeks_views(prints, prints_lw):
    # Get total views in the last 3 weeks
    views_lw = prints.selectExpr(
        "user_id as user_id_lw", 
        "value_prop as value_prop_lw", 
        "day as lw_day"
    )
    prints_l3w_views = (
        prints_lw.join(
            views_lw,
            (views_lw["lw_day"] > prints["day"])
            & (views_lw["lw_day"] < date_sub(prints_lw["day"], 21))
            & (prints_lw["user_id"] == views_lw["user_id_lw"])
            & (prints_lw["value_prop"] == views_lw["value_prop_lw"]),
            "left",
        )
        .withColumn("view_last_3_weeks", col("lw_day").isNotNull().cast("int"))
        .groupBy(col("value_prop"), col("user_id"), col("day"))
        .sum()
        .selectExpr(
            "day as view_l3w_day",
            "value_prop as view_l3w_value_prop",
            "user_id as view_l3w_user_id",
            "`sum(view_last_3_weeks)` as total_views_last_3_weeks",
        )
    )
    return prints_l3w_views

def get_three_weeks_taps(prints_lw, taps):
    # Get total taps in the last 3 weeks
    prints_l3w_taps = prints_lw.join(
                        taps,
                        (prints_lw["day"] > taps["tap_day"])
                        & (taps["tap_day"] < date_sub(prints_lw["day"], 21))
                        & (prints_lw["user_id"] == taps["tap_user_id"])
                        & (prints_lw["value_prop"] == taps["tap_value_prop"]),
                        "left",)\
                    .withColumn("tap_last_3_weeks", col("tap_day").isNotNull().cast("int"))\
                    .agroupBy(col("value_prop"), col("user_id"), col("day"))\
                    .sum().selectExpr(
                        "day as tap_l3w_day",
                        "value_prop as tap_l3w_value_prop",
                        "user_id as tap_l3w_user_id",
                        "`sum(tap_last_3_weeks)` as total_taps_last_3_weeks",
                    )
    return prints_l3w_taps

def get_three_week_payments(prints_lw, payments):
    number_payments = (
        prints_lw.join(
            payments,
            (prints_lw["day"] == payments["pay_date"])
            & (prints_lw["user_id"] == payments["pay_user_id"])
            & (prints_lw["value_prop"] == payments["pay_value_prop"]),
            "left",
        )
        .withColumn("pay", col("pay_date").isNotNull().cast("int"))
        .groupBy(col("value_prop"), col("user_id"), col("day"))
        .sum()
        .selectExpr(
            "day as pay_day",
            "value_prop as pay_value_prop",
            "user_id as pay_user_id",
            "`sum(pay)` as number_payments_l3w",
        )
    )
    return number_payments

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
    prints_lw = get_last_week(prints)
    tapped_lw = get_last_week_tapped(prints_lw, taps)
    views_l3w = get_last_three_weeks_views(prints, prints_lw)
    taps_l3w = get_three_weeks_taps(prints_lw, taps)
    payments_l3w = get_three_week_payments(prints_lw, payments)

if __name__=='__main__':
    main()
