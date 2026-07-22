'''
Problem
Social Media PII Extraction.

You are a data engineer on the privacy engineering team at Meta. Raw user contact data cannot flow into the analytics warehouse as-is: phone numbers must be masked and email addresses reduced to just their domain so analysts can study provider distribution without seeing PII. Write a query against social_media_pii_input (note: all three columns are stored as text) that produces, for every user: email_domain, the part of email after the @ sign; anon_phone, the literal string ****** followed by the last 4 digits of phone (the phones are 10-digit strings, so the first six digits are masked); and user_id cast to an integer. Return the columns in the order anon_phone, email_domain, user_id, sorted by anon_phone in ascending order.

Schema columns: social_media_pii_input.user_id, social_media_pii_input.email, social_media_pii_input.phone

Output columns: anon_phone, email_domain, user_id '''

### Solution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
    return (
        social_media_pii_input
        .select(
            F.concat(
                F.lit("******"),
                F.substring("phone", 7, 4)
            ).alias("anon_phone"),
            F.split("email", "@")[1].alias("email_domain"),
            F.col("user_id").cast("int").alias("user_id")
        )
        .orderBy("anon_phone")
    )
