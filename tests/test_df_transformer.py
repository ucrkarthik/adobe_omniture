import os
from adobe.omniture.utils.csv import csv_read
from adobe.omniture.se_revenue.schema import data_schema
from adobe.omniture.se_revenue import se_revenue_driver

import pytest
from pyspark.sql import SparkSession

from adobe.omniture.se_revenue import se_revenue_driver
from adobe.omniture.utils.logger import Logger

# This allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_session")

# Get current directory location
dir_name, filename = os.path.split(os.path.abspath(__file__))

# Update test config args
source_data = os.path.join(dir_name, 'resources/data.sql')

job_args = {
    'source': source_data,
    'app_name': 'pytest SE Revenue'}

# Create an instance of Logger
logger = Logger.get_logger("test_data_transform train")


def test_unpack_product_list_and_capture_search_keyword(spark_session: SparkSession) -> None:
    """
    Test the se_revenue_driver run method.
    :param spark_session: Instance of the Spark Session
    :return:
    """

    # Create Dataframe from source
    hit_level_df = csv_read(spark_session, data_schema(), job_args['source']).cache()

    # Call unpack_product_list method
    hit_level_df = se_revenue_driver.unpack_product_list(hit_level_df)

    assert all(x in hit_level_df.columns for x in
               ['pl_category', 'pl_product_name', 'pl_number_of_items', 'pl_total_revenue', 'pl_custom_event',
                'pl_merchandising_evar', 'referrer'])

    assert "www.google.com" == \
           hit_level_df.select("search_engine_domain")\
                        .where("hit_time_gmt == 1254033280").collect()[0]["search_engine_domain"]


    hit_level_df = se_revenue_driver.capture_search_keyword(hit_level_df)

    assert "Ipod" == \
           hit_level_df.select("search_keyword")\
                        .where("hit_time_gmt == 1254033280").collect()[0]["search_keyword"]

    assert "cd player" == \
           hit_level_df.select("search_keyword")\
                        .where("hit_time_gmt == 1254033478").collect()[0]["search_keyword"]
