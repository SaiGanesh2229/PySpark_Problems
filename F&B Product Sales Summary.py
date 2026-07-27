'''
Problem
A food-and-beverage retailer needs one summary row for every product, including products with no sales. Report total quantity and revenue from sales plus total stock across warehouses, using zero when a product has no matching activity.

Schema columns:

fnb_products.product_id, fnb_products.name, fnb_products.category
fnb_sales.sale_id, fnb_sales.product_id, fnb_sales.quantity, fnb_sales.revenue
fnb_inventory.product_id, fnb_inventory.stock, fnb_inventory.warehouse
Output columns: product_id, name, category, total_quantity, total_revenue, total_stock
'''

## Solution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(inventory, products, sales):
    sales_agg = (
        fnb_sales
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("revenue").alias("total_revenue")
        )
    )
    inventory_agg = (
        fnb_inventory
        .groupBy("product_id")
        .agg(
            F.sum("stock").alias("total_stock")
        )
    )
    result = (
        fnb_products
        .join(sales_agg, "product_id", "left")
        .join(inventory_agg, "product_id", "left")
        .select(
            "category",
            "name",
            "product_id",
            F.coalesce(F.col("total_quantity"), F.lit(0)).alias("total_quantity"),
            F.coalesce(F.col("total_revenue"), F.lit(0)).alias("total_revenue"),
            F.coalesce(F.col("total_stock"), F.lit(0)).alias("total_stock")
        )
        .orderBy("category", "name")
    )
    return result
