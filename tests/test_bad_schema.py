import os
from adobe.omniture.utils.csv import csv_read
from adobe.omniture.se_revenue.schema import data_schema

import pytest
from pyspark.sql import SparkSession

from adobe.omniture.se_revenue import se_revenue_driver
from adobe.omniture.utils.logger import Logger

# This allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_session")

# Get current directory location
dir_name, filename = os.path.split(os.path.abspath(__file__))

# Update test config args
source_data = os.path.join(dir_name, 'resources/data_bad_schema.sql')

job_args = {
    'source': source_data,
    'app_name': 'pytest SE Revenue'}

# Create an instance of Logger
logger = Logger.get_logger("test_data_transform train")


def test_schema_check(spark_session: SparkSession) -> None:
    """
    Test the se_revenue_driver run method.
    :param spark_session: Instance of the Spark Session
    :return:
    """

    # Create Dataframe from source
    hit_level_df = csv_read(spark_session, data_schema(), job_args['source']).cache()

    corrupt_rec_count = hit_level_df.filter(hit_level_df._corrupt_record.isNotNull()).count()

    logger.info(f"Corrupt Recoreds Received: {corrupt_rec_count}")

    assert corrupt_rec_count == 1
