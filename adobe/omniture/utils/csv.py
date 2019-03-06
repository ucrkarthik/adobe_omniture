from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def csv_read(sc: SparkSession, schema: StructType, source_path: str):
    """
    Method used to read the CSV file and validate the data vs a schema.
    If any records were malformed based on the schema that was provided,
    it will store that record in the columnNameOfCorruptRecord column.
    :param sc: Instance of a spark session
    :param schema: Schema used to validate the CSV inbound file.
    :param path: Path to where the CSV file is located.
    :return: Dataframe containing the results.
    """
    return sc.read.option("header", "true") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .option("mode", "PERMISSIVE") \
        .option("delimiter", "\t") \
        .schema(schema) \
        .csv(source_path)
