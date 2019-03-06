from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


def data_schema():
    schema = StructType([StructField("hit_time_gmt", IntegerType(), nullable=False),
                         StructField("date_time", TimestampType(), nullable=False),
                         StructField("user_agent", StringType(), nullable=False),
                         StructField("ip", StringType(), nullable=False),
                         StructField("event_list", IntegerType(), nullable=True),
                         StructField("geo_city", StringType(), nullable=False),
                         StructField("geo_region", StringType(), nullable=False),
                         StructField("geo_country", StringType(), nullable=False),
                         StructField("pagename", StringType(), nullable=False),
                         StructField("page_url", StringType(), nullable=False),
                         StructField("product_list", StringType(), nullable=True),
                         StructField("referrer", StringType(), nullable=False),
                         StructField("_corrupt_record", StringType(), nullable=True)])
    return schema
