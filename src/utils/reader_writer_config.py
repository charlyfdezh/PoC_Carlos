"""
Util classes for reading and writing files in different formats using PySpark.
"""
import dataclasses
import logging
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from src.utils.logger_config import LoggerConfig

logger = LoggerConfig.configure_logger(log_file_path="application.log",
                                       log_level=logging.INFO)


@dataclasses.dataclass
class Writer:
    """
    Class to write files in different formats.
    """

    @staticmethod
    def write_file(spark: SparkSession,
                   df: DataFrame,
                   output_path: str,
                   file_format: str = "parquet",
                   mode: str = "overwrite",
                   schema: StructType = None) -> None:
        """
        Writes a DataFrame to a file in the specified format with
        an optional schema.

        :param df: PySpark DataFrame to write.
        :param output_path: Output file path.
        :param file_format: File format (parquet, csv, json, etc.).
        :param mode: Write mode (overwrite, append, etc.).
        :param schema: Optional schema to enforce when writing the file.
        :return: None
        :raises IOError: If the file cannot be written.
        :raises ValueError: If the file format is not supported.
        """
        try:
            if schema:
                logger.info("Applying schema before writing the file.")
                df = spark.createDataFrame(df.rdd, schema)

            df.write.mode(mode).parquet(output_path)

            logger.info("File written successfully at %s with %s format",
                        os.path.relpath(output_path), file_format)
        except (IOError, ValueError) as e:
            logger.error("Error writing file: %s", e)

    @staticmethod
    def write_delta_table(df: DataFrame,
                          table_path: str,
                          mode: str = "overwrite",
                          partition_by: list = None) -> None:
        """
        Writes a DataFrame to a Delta Lake table.

        :param df: PySpark DataFrame to write.
        :param table_path: Path to the Delta table.
        :param mode: Write mode (overwrite, append, etc.).
        :param partition_by: List of columns to partition the table by.
        :return: None
        :raises IOError: If the table cannot be written.
        :raises ValueError: If the table path is invalid or if the mode is not supported.
        """
        try:
            writer = df.write.format("delta").mode(mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(table_path)
            relative_path = os.path.relpath(table_path)
            logger.info("Delta table written successfully to %s.", relative_path)
        except (IOError, ValueError) as e:
            logger.error("Error writing Delta table: %s", e)


@dataclasses.dataclass
class Reader:
    """
    Class to read files in different formats.
    """
    @staticmethod
    def read_file(spark: SparkSession,
                  input_path: str,
                  file_format: str = "csv",
                  options: dict = None,
                  schema: StructType = None) -> DataFrame:
        """
        Reads a file in the specified format and returns a DataFrame.

        :param spark: SparkSession instance.
        :param input_path: Input file path.
        :param file_format: File format (parquet, csv, json, etc.).
        :param options: Additional options for reading
                        (e.g., header=True for CSV).
        :param schema: Optional schema to enforce when reading the file.
        :return: PySpark DataFrame.
        :raises IOError: If the file cannot be read.
        :raises ValueError: If the file format is not supported.
        """
        try:
            if options is None:
                options = {}

            if schema:
                df = (spark.read.format(file_format)
                      .options(**options)
                      .schema(schema)
                      .load(input_path))
            else:
                df = (spark.read.format(file_format)
                      .options(**options)
                      .load(input_path))

            logger.info("File read successfully from %s with %s format",
                        os.path.relpath(input_path), file_format)
            return df
        except (IOError, ValueError) as e:
            logger.error("Error reading file: %s", e)
            return None

    @staticmethod
    def read_olap(spark: SparkSession,
                  connection_string: str,
                  query: str) -> DataFrame:
        """
        Reads data from an OLAP cube using a JDBC connection.

        :param spark: SparkSession instance.
        :param connection_string: JDBC connection string for the OLAP source.
        :param query: SQL query to extract data from the OLAP cube.
        :return: PySpark DataFrame with the extracted data.
        """
        try:
            df = (spark.read.format("jdbc")
                  .option("url", connection_string)
                  .option("query", query)
                  .load())
            logger.info("Data successfully read from OLAP cube.")
            return df
        except Exception as e:
            logger.error("Error reading data from OLAP cube: %s", e)
            raise
