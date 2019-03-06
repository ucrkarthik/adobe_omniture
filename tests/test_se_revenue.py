import os
import sys
import pytest
from pyspark.sql import SparkSession

from adobe.omniture.utils.logger import Logger
from adobe.omniture.se_revenue import se_revenue_driver

# This allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_session")

# Get current directory location
dir_name, filename = os.path.split(os.path.abspath(__file__))

# Update test config args
ser_data = os.path.join(dir_name, 'resources/data.sql')

job_args = {
    'source': ser_data,
    'app_name': 'pytest SE Revenue'}

# Create an instance of Logger
logger = Logger.get_logger("test_data_transform train")


def test_data_transform_train_runjob(spark_session: SparkSession) -> None:
    """
    Test the se_revenue_driver run method.
    :param spark_session: Instance of the Spark Session
    :return:
    """

    raw_df = se_revenue_driver.run_job(spark_session, logger, job_args)

    logger.info(f"raw_df: {raw_df.count()}")

    assert raw_df.count() == 21
