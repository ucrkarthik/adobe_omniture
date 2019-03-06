import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from pyspark.sql.functions import split, when, regexp_replace, col
from adobe.omniture.utils.logger import Logger
from adobe.omniture.utils.arg_parse import ArgParser
from adobe.omniture.utils.csv import csv_read
from adobe.omniture.se_revenue.schema import data_schema

def run_job(spark: SparkSession, logger: Logger, job_args: dict) ->  DataFrame:

    # Create Dataframe from source
    raw_df = csv_read(spark, data_schema(), job_args['source']).cache()

    logger.info(f"Corrupt Recored Received: {raw_df.filter(raw_df._corrupt_record.isNotNull()).count()}")

    # Only capture records that are not corrupt
    raw_df = raw_df.filter(raw_df._corrupt_record.isNull()).drop(raw_df._corrupt_record)

    logger.info(f"Recored Received: {raw_df.count()}")

    # Move all the Product List into a separate columns
    raw_df = raw_df.select(
        '*',
        split("product_list", ";")[0].alias("pl_category"),
        split("product_list", ";")[1].alias("pl_product_name"),
        split("product_list", ";")[2].alias("pl_number_of_items"),
        split("product_list", ";")[3].alias("pl_total_revenue"),
        split("product_list", ";")[4].alias("pl_custom_event"),
        split("product_list", ";")[5].alias("pl_merchandising_evar"),
        regexp_replace("referrer", "^((http[s]?|ftp):\/)?\/?([^:\/\s]+)((\/\w+)*\/)([\w\-\.]+[^#?\s]+)(.*)?(#[\w\-]+)?",
                       "$3").alias("referral_name")
    ).where("referrer not like '%esshopzilla%'")

    # regexp_replace("referrer", "^((http[s]?|ftp):\/)?\/?([^:\/\s]+)((\/\w+)*\/)([\w\-\.]+[^#?\s]+)(.*)?(#[\w\-]+)?",
    #                "$3").alias("referral_name")

    # Set empty spaces in a cell to NULL
    for column in raw_df.columns:
        raw_df = raw_df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))

    raw_df.printSchema()
    raw_df.show(truncate=False)

    return raw_df


def main(main_args: list) -> None:
    """
    Search Engine main method:
    :param main_args: Contains arguments passed into the spark-submit
    :return:
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

        # TODO: Save the file and rename it
        logger.info(f"raw_df: {raw_df.count()}")

    except Exception as ex:

        logger.error(ex)

        raise

if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
