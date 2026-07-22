'''
Problem
A property manager wants the total monthly rent represented by each landlord's portfolio. For every landlord with at least one property, return the landlord id, full name, and the sum of rent across all their properties.

Schema columns: prop_landlords.landlord_id, prop_landlords.first_name, prop_landlords.last_name, prop_landlords.email, prop_landlords.phone, prop_properties.property_id, prop_properties.landlord_id, prop_properties.property_type, prop_properties.rent, prop_properties.square_feet, prop_properties.city

Output columns: landlord_id, landlord_name, total_rental_income. '''

### Solution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(prop_landlords, prop_properties):
    result = (
        prop_properties.alias("p")
        .join(
            prop_landlords.alias("l"),
            F.col("p.landlord_id") == F.col("l.landlord_id"),
            "inner"
        )
        .groupBy(
            F.col("l.landlord_id"),
            F.col("l.first_name"),
            F.col("l.last_name")
        )
        .agg(
            F.sum("p.rent").alias("total_rental_income")
        )
        .select(
            F.col("landlord_id"),
            F.concat_ws(
                " ",
                F.col("first_name"),
                F.col("last_name")
            ).alias("landlord_name"),
            F.col("total_rental_income")
        )
    )
    return result
