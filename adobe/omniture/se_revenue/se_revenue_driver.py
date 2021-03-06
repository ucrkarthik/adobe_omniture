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


from typing import List, Tuple
import pandas as pd
import shutil, os

def create_excel_spreadsheet(dfs: List[Tuple[str, pd.DataFrame]], target_path: str) -> None:
    """
    Takes a list of tuples (report names, pandas dataframes) and creates an xlsx file, where each tuple is
    a seperate sheet.
    :param dfs: list of tuples - names and pandas dataframes, e.g. [('adherence', adherence_df), ...]
    :param s3_path: desired s3 location for the xlsx file
    :return:
    """
    local_filename = datetime.now().strftime("%Y-%m-%d") + '_SearchKeywordPerformance.xlsx'
    excel_writer = pd.ExcelWriter(local_filename, engine='xlsxwriter')
    for i, tup in enumerate(dfs):
        tup[1].to_excel(excel_writer, sheet_name=tup[0])
    excel_writer.save()

    if("s3://" in target_path):
        os.system(f"aws s3 cp {local_filename} {target_path}{local_filename} --sse --acl bucket-owner-full-control")
    else:
        shutil.move(local_filename, target_path+local_filename)  # add source dir to filename


def unpack_product_list(hit_level_df: DataFrame) -> DataFrame:
    """
    Parse the Product List semicolon separate values into a separate columns
    :param hit_level_df: DataFrame containing the click data
    :return: Click data eataFrame containing all the columns plus the product list data in its own columns
    """
    return hit_level_df.select(
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

def capture_search_keyword(hit_level_df: DataFrame) -> DataFrame:
    """
    Locate the search engin keywords in the referrer URL and store it in a new column
    :param hit_level_df: DataFrame containing the click data
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
    return hit_level_df.withColumn("search_keyword", url_paramater_udf(hit_level_df.referrer))


def run_job(spark: SparkSession, logger: Logger, job_args: dict) ->  DataFrame:
    """
    The run_job method is called by the main method and by the pytest file. It extracts and transforms the
    the "hit level data" and calculates the how much revenue the client is getting from external Search Engines( such as
    Google, Yahoo and MSN) and which keywords are performing the best based on revenue.
    The method will return a dataframe which will have the following schema:
        root
          |-- Search Engine Domain: string (nullable = true)
          |-- Search Keyword: string (nullable = true)
          |-- Revenue: string (nullable = true)

    Here is an example of the output:
        +--------------------+--------------+-------+
        |Search Engine Domain|Search Keyword|Revenue|
        +--------------------+--------------+-------+
        |      www.google.com|          Ipod|    290|
        |        www.bing.com|          Zune|    250|
        |      www.google.com|          ipod|    190|
        +--------------------+--------------+-------+

    :param spark: Spark session
    :param logger: Logger object for logging messages. Default is INFO.
    :param job_args: Job arguments passed in by spark-submit: source and target path
    :return: DataFrame containing the results
    """

    # Create Dataframe from source
    hit_level_df = csv_read(spark, data_schema(), job_args['source']).cache()

    logger.info(f"Corrupt Recoreds Received: {hit_level_df.filter(hit_level_df._corrupt_record.isNotNull()).count()}")

    # Only capture records that are not corrupt
    hit_level_df = hit_level_df.filter(hit_level_df._corrupt_record.isNull()).drop(hit_level_df._corrupt_record)

    logger.info(f"Records Processed: {hit_level_df.count()}")

    # Handle the Product List column
    hit_level_df = unpack_product_list(hit_level_df)

    # Parse the keyword used in the referrer URL
    hit_level_df = capture_search_keyword(hit_level_df)

    # Set empty spaces in a cell to NULL
    for column in hit_level_df.columns:
        hit_level_df = hit_level_df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))

    # Use window function to help do operations within the "ip" partition(aka group).
    window_spec = Window.partitionBy(hit_level_df["ip"]).orderBy(hit_level_df["hit_time_gmt"])
    hit_level_df = hit_level_df.select(first("search_engine_domain", ignorenulls=True).over(window_spec).alias("Search Engine Domain"),
                           first("search_keyword", ignorenulls=True).over(window_spec).alias("Search Keyword"),
                           last("pl_total_revenue", ignorenulls=True).over(window_spec).alias("Revenue"))\
                   .where(col("event_list")==1).orderBy("Revenue", ascending=False)

    return hit_level_df


def main(main_args: list) -> None:
    """
    Search Engine Revenue calculator main method. It create the session and calls the run_job method to
    calculate the results. The results are then saved as a CSV file in the target(argument passed in) folder path.
    :param main_args: Contains arguments passed into the spark-submit(source, target, app_name)
    :return: None
    """
    logger = Logger.get_logger(os.path.splitext(os.path.basename(__file__))[0])

    logger.info(f"main_args: {main_args}")

    # Use general arg parser list to specify the arguments being passed
    job_args = vars(ArgParser.general_arg_parser_list().parse_args(main_args[1:]))

    logger.info(f"Type job_args: {type(job_args)}")
    logger.info(f"Job Args: {job_args}")

    try:
        # Create a spark session
        spark = SparkSession.builder.appName(job_args["app_name"]).getOrCreate()

        # Run the Datatransform
        search_engin_rev_results_df = run_job(spark, logger, job_args)

        # Store the file in a tsv format
        search_engin_rev_results_df.toPandas().to_csv(job_args['target']+"/"+ datetime.now().strftime("%Y-%m-%d") + '_SearchKeywordPerformance.tsv', sep="\t")

        # Example of how to store it in a excel file
        #pandas_df_list = []
        #pandas_df_list.append(("Search Engin Rev Results", search_engin_rev_results_df.toPandas()))
        #create_excel_spreadsheet(pandas_df_list, job_args['target'])

    except Exception as ex:

        logger.error(ex)

        raise

if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
