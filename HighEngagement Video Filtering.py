'''
Problem
You are working at a video streaming platform. Your team wants to highlight popular, recent videos on the homepage to boost engagement. Given a table of video metadata, return the videos that have more than 1,000,000 views and were released in 2019 or later (release_year >= 2019).

Schema columns:

video_stream: video_id, title, genre, release_year, duration, view_count
Output columns: duration, genre, release_year, title, video_id, view_count

Sort the results by duration in ascending order. '''

### Solution
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(video_stream_df):
    result_df = (
    video_stream
    .filter(
        (F.col("view_count") > 1000000) &
        (F.col("release_year") >= 2019)
    )
    .select(
        "duration",
        "genre",
        "release_year",
        "title",
        "video_id",
        "view_count"
    )
    .orderBy(F.col("duration").asc())
    )
    
    return result_df
