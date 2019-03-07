import os
from datetime import datetime

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
target_data = os.path.join(dir_name, 'results/DATE_SearchKeywordPerformance.tab')

job_args = {
    'source': source_data,
    'target': target_data,
    'app_name': 'pytest SE Revenue'}

# Create an instance of Logger
logger = Logger.get_logger("test_se_revenue")


def test_se_revenue_pipeline(spark_session: SparkSession) -> None:
    """
    Test the se_revenue_driver run method.
    :param spark_session: Instance of the Spark Session
    :return:
    """

    #Call the se_revenue_driver run method
    raw_df = se_revenue_driver.run_job(spark_session, logger, job_args)

    raw_df.printSchema()
    raw_df.show()

    #Store the results locally
    target_path = job_args["target"].replace("DATE", datetime.now().strftime("%Y-%m-%d"))
    raw_df.repartition(1).write.option('header', 'true').mode('overwrite').csv(target_path)

    assert raw_df.count() == 3
