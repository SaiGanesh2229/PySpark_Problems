'''
Problem
Build a daily call-volume summary for the support floor.

You are a data analyst at a telecommunications company. The call-center operations lead wants a per-day summary of how many distinct customers called in and how much total talk time was logged, so staffing can be matched to demand. Note that this raw feed lands with every column stored as text, so numeric values must be cast before aggregating.

Write a query to summarize calls per day. Join cc_calls to cc_customer on cust_id so that only calls from known customers are counted. Group by date and compute: the number of distinct customers who called that day (num_customers), and the total call duration that day (total_duration), casting duration from text to integer before summing. Return both aggregates as integers and sort the results by date in ascending order.

Schema columns: cc_calls.call_id, cc_calls.cust_id, cc_calls.date, cc_calls.duration, cc_customer.cust_id, cc_customer.name, cc_customer.state, cc_customer.tenure, cc_customer.occupation

Output columns: date, num_customers, total_duration '''

### Solution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(calls_df, customers_df):
    # Write code here
    result = (
        cc_calls.alias("cls")
        .join(
            cc_customer.alias("csm"),
            F.col("cls.cust_id") == F.col("csm.cust_id"),
            "inner"
        )
        .groupBy("cls.date")
        .agg(
            F.countDistinct("cls.cust_id").cast("int").alias("num_customers"),
            F.sum(F.col("cls.duration").cast("int")).cast("int").alias("total_duration")
        )
        .orderBy("cls.date")
    )
    return result
