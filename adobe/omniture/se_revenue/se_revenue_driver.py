import os
from datetime import datetime
from urllib.parse import urlsplit, parse_qs

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import split, when, regexp_replace, col, udf, last, first
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from adobe.omniture.se_revenue.schema import data_schema
from adobe.omniture.utils.arg_parse import ArgParser
from adobe.omniture.utils.csv import csv_read
from adobe.omniture.utils.logger import Logger


def unpack_product_list(click_df: DataFrame) -> DataFrame:
    """
    Parse the Product List semicolon separate values into a separate columns
    :param click_df: DataFrame containing the click data
    :return: Click data eataFrame containing all the columns plus the product list data in its own columns
    """
    return  click_df.select(
        '*',
        split("product_list", ";")[0].alias("pl_category"),
        split("product_list", ";")[1].alias("pl_product_name"),
        split("product_list", ";")[2].alias("pl_number_of_items"),
        split("product_list", ";")[3].alias("pl_total_revenue"),
        split("product_list", ";")[4].alias("pl_custom_event"),
        split("product_list", ";")[5].alias("pl_merchandising_evar"),
        regexp_replace("referrer", "^((http[s]?|ftp):\/)?\/?([^:\/\s]+)((\/\w+)*\/)([\w\-\.]+[^#?\s]+)(.*)?(#[\w\-]+)?",
                       "$3").alias("search_engine_domain")
    )

def capture_search_keyword(click_df: DataFrame) -> DataFrame:
    """
    Locate the search engin keywords in the referrer URL and store it in a new column
    :param click_df: DataFrame containing the click data
    :return: Click data eataFrame containing all the columns plus the keyword column
    """

    def parse_url(url: str) -> str:
        """
        Method used by the UDF to return the keyword paramater from the URL
        :param url: String containing the URL
        :return:
        """
        parameters = parse_qs(urlsplit(url).query)
        # used in google and bing to hold the keyward search value
        if 'q' in parameters:
            return parameters['q'][0]
        # used in yahoo to hold the keyward search value
        elif 'p' in parameters:
            return parameters['p'][0]
        else:
            return None

    # Get the keyword search value stored in the URL parameter
    url_paramater_udf = udf(lambda x: parse_url(x), StringType())
    return click_df.withColumn("search_keyword", url_paramater_udf(click_df.referrer))


def run_job(spark: SparkSession, logger: Logger, job_args: dict) ->  DataFrame:
    """
    The execution method
    :param spark: spark session
    :param logger: logger object to logging
    :param job_args: Job arguments passed in by spark-submit
    :return:
    """

    # Create Dataframe from source
    raw_df = csv_read(spark, data_schema(), job_args['source']).cache()

    logger.info(f"Corrupt Recored Received: {raw_df.filter(raw_df._corrupt_record.isNotNull()).count()}")

    # Only capture records that are not corrupt
    raw_df = raw_df.filter(raw_df._corrupt_record.isNull()).drop(raw_df._corrupt_record)

    logger.info(f"Recored Received: {raw_df.count()}")

    # Handle the Product List column
    raw_df = unpack_product_list(raw_df)

    # Parse the keyword used in the referrer URL
    raw_df = capture_search_keyword(raw_df)

    # Set empty spaces in a cell to NULL
    for column in raw_df.columns:
        raw_df = raw_df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))

    # Use window function to help do operations within the "ip" partition(aka group).
    window_spec = Window.partitionBy(raw_df["ip"]).orderBy(raw_df["hit_time_gmt"])
    raw_df = raw_df.select(first("search_engine_domain", ignorenulls=True).over(window_spec).alias("Search Engine Domain"),
                           first("search_keyword", ignorenulls=True).over(window_spec).alias("Search Keyword"),
                           last("pl_total_revenue", ignorenulls=True).over(window_spec).alias("Revenue"))\
                   .where(col("event_list")==1).orderBy("Revenue", ascending=False)

    raw_df.printSchema()
    raw_df.show(truncate=False)

    return raw_df


def main(main_args: list) -> None:
    """
    Search Engine Revenue calculator main methold. It takes
    :param main_args: Contains arguments passed into the spark-submit(source, target, app_name)
    :return: None
    """
    logger = Logger.get_logger(os.path.splitext(os.path.basename(__file__))[0])

    # Use general arg parser list to specify the arguments being passed
    job_args = ArgParser.general_arg_parser_list().parse_args(main_args)

    logger.info("Config Args: {}".format(job_args))

    try:
        # Create a spark session
        spark = SparkSession.builder.appName(job_args["app_name"]).getOrCreate()

        # Run the Datatransform
        raw_df = run_job(spark, logger, job_args)

        target_path = job_args["target"].replace("DATE", datetime.now().strftime("%Y-%m-%d"))
        raw_df.repartition(1).write.option('header', 'true').mode('overwrite').csv(target_path)

    except Exception as ex:

        logger.error(ex)

        raise

if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
