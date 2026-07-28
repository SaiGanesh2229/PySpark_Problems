'''
Problem
Consolidate two regional customer books into one list.

You are a data engineer at Databricks working with an insurance client. The client's customer records live in two separate tables that came from different regional systems, ic_data_1 and ic_data_2, with identical schemas. They want a single consolidated list for the underwriting team.

Write a query that combines all rows from ic_data_1 and ic_data_2 using a UNION ALL — do not deduplicate; every row from both tables must appear in the result. Return the columns customer_id, first_name, last_name, age, and policy_type, and sort the combined result by age in ascending order.

Schema columns: ic_data_1.customer_id, ic_data_1.first_name, ic_data_1.last_name, ic_data_1.age, ic_data_1.policy_type, ic_data_2.customer_id, ic_data_2.first_name, ic_data_2.last_name, ic_data_2.age, ic_data_2.policy_type

Output columns: customer_id, first_name, last_name, age, policy_type
'''

## Solution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(ic_data_1, ic_data_2):
    result = (
        ic_data_1.union(ic_data_2).orderBy("age")
    )
    return result
