'''
Problem
A manufacturing analytics team wants to see how each product ranks by revenue within its own category. For every product that has a matching sales record, report the product's category, its name, its sale revenue rounded to the nearest whole number, and its rank inside that category. Rank 1 is the product with the highest revenue in the category; products with equal revenue share the same rank and the next rank is skipped accordingly. Products with no sales record do not appear.

Schema columns:

manufacture_product: product_id, category, product_name
manufacture_sales: sale_id, product_id, quantity, revenue
Output columns: category, product_name, rank, revenue '''

### Solution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(products, sales):
    # Write code here
    window_spec = W.partitionBy("category").orderBy(F.col("revenue").desc())
    result = (
        manufacture_product.alias("p")
        .join(
            manufacture_sales.alias("a"),
            F.col("p.product_id") == F.col("a.product_id"),
            "inner"
        )
        .select(
            F.col("p.category").alias("category"),
            F.col("p.product_name").alias("product_name"),
            F.rank().over(window_spec).alias("rank"),
            F.col("a.revenue").alias("revenue")
        )
    )
    return result
