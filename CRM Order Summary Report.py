'''
Problem
You’re working as a Data Engineer at a company that builds Customer Relationship Management (CRM) software. Your goal is to build a unified view that shows order details along with customer and product information — useful for internal dashboards and reporting. You are given three datasets (or tables) that store customer, order, and product details.

Schema columns:

crm_customers: customer_id, first_name, last_name, email
crm_orders: order_id, customer_id, product_id, order_date
crm_products: product_id, product_name, category
Output columns: order_id, customer_name, customer_email, product_name, product_category, order_date '''

### Solution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
    result = (
        orders.alias("o")
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "inner"
        )
        .join(
            products.alias("p"),
            F.col("o.product_id") == F.col("p.product_id"),
            "inner"
        )
        .select(
            F.col("o.order_id").alias("order_id"),
            F.concat_ws(
                " ",
                F.col("c.first_name"),
                F.col("c.last_name")
            ).alias("customer_name"),
            F.col("c.email").alias("customer_email"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.category").alias("product_category"),
            F.col("o.order_date").alias("order_date")
            )
    )
    return result
