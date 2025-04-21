"""
Validation
"""
import dataclasses
import logging
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import IntegerType, StringType, DoubleType
from src.utils.reader_writer_config import Reader
from src.utils.logger_config import LoggerConfig

logger = LoggerConfig.configure_logger(log_file_path="application.log",
                                       log_level=logging.INFO)
BASE_PATH = os.path.abspath("c:/Users/Usuario/Desktop/NN/poc-nn-carlos/resources")
RAW_ZONE_PATH = os.path.join(BASE_PATH, "raw_zone")
DATAHUB_ZONE_PATH = os.path.join(BASE_PATH, "datahub")


@dataclasses.dataclass
class DataValidator:
    """
    Class to validate data in a DataFrame against a metadata table.
    """

    @staticmethod
    def validate_data_types(df: DataFrame,
                            metadata_df: DataFrame) -> tuple[DataFrame, list]:
        """
        Validates the data types of a DataFrame using the metadata DataFrame.
    
        :param df: DataFrame to validate.
        :param metadata_df: DataFrame containing metadata information.
        :return: A tuple containing the DataFrame with valid data types
        and a list of errors.
        """
        metadata_df = metadata_df.selectExpr(
            "trim(field_name) as field_name", "trim(field_type) as field_type")
        type_mapping = {"Integer": IntegerType(),
                        "String": StringType(),
                        "Double": DoubleType()}
        error_list = []

        for row in metadata_df.collect():
            field_name, field_type = row["field_name"], row["field_type"]
            expected_type = type_mapping.get(field_type)

            if not expected_type:
                continue

            if field_name not in df.columns:
                error_list.append({
                    "field_name": field_name,
                    "error": f"Field '{field_name}' not found in DataFrame"
                })
                continue

            try:
                invalid_records_df = df.filter(~col(field_name)
                                               .cast(expected_type).isNotNull())
                if invalid_records_df.count() > 0:
                    error_list.append({
                        "field_name": field_name,
                        "error": f"Invalid data type for field '{field_name}'",
                        "invalid_records": invalid_records_df.collect()
                    })
                    df = df.subtract(invalid_records_df)
            except (TypeError, ValueError) as e:
                error_list.append({
                    "field_name": field_name,
                    "error": f"Error casting field '{field_name}': {str(e)}"
                })

        return df, error_list

    @staticmethod
    def validate_primary_keys(df: DataFrame,
                              metadata_df: DataFrame) -> tuple[DataFrame, list]:
        """
        Validates primary keys in a DataFrame using the metadata DataFrame.
    
        :param df: DataFrame to validate.
        :param metadata_df: DataFrame containing metadata information.
        :return: A tuple containing the DataFrame with valid data
        and a list of errors.
        """
        primary_keys = (metadata_df.filter(col("key").isNotNull())
                        .select("field_name", "key").collect())
    
        error_list = []
    
        key_groups = {}
        for row in primary_keys:
            field_name = row["field_name"].strip()
            key_level = row["key"].strip()
            if key_level not in key_groups:
                key_groups[key_level] = []
            key_groups[key_level].append(field_name)
    
        for key_level, fields in key_groups.items():
            duplicate_df = (df.groupBy(fields)
                            .agg(count("*").alias("count"))
                            .filter(col("count") > 1))
    
            if duplicate_df.count() > 0:
                error_list.append({
                    "key_level": key_level,
                    "error": f"Duplicate values found for primary key level '{key_level}'",
                    "fields": fields
                })
    
                duplicate_keys = duplicate_df.select(fields).distinct()
                df = df.join(duplicate_keys, fields, "left_anti")
    
            for field in fields:
                null_df = df.filter(col(field).isNull())
                if null_df.count() > 0:
                    error_list.append({
                        "key_level": key_level,
                        "error": f"Null values found in primary key field '{field}'",
                        "fields": [field]
                    })
    
                    df = df.filter(col(field).isNotNull())
    
        return df, error_list

    @staticmethod
    def validate_data(spark: SparkSession,
                      df: DataFrame,
                      metadata_path: str,
                      validation_strategy: str = "reject_all") -> DataFrame:
        """
        Validates the data and ingests it based on the chosen strategy.
    
        :param spark: SparkSession instance.
        :param df: DataFrame to validate.
        :param metadata_path: Path to the metadata file.
        :param validation_strategy: Strategy for handling validation results.
                                     Options: "reject_all", "ingest_valid".
        """
        all_errors = []

        metadata_df = Reader.read_file(
            spark, metadata_path, file_format="csv", options={"header": "true"})

        if validation_strategy == "reject_all":
            valid_data, type_errors = DataValidator.validate_data_types(df, metadata_df)
            all_errors.extend(type_errors)

            valid_data, primary_key_errors = DataValidator.validate_primary_keys(valid_data, metadata_df)
            all_errors.extend(primary_key_errors)

            if all_errors:
                logger.error("Validation failed. No data will be ingested.")
                for error in all_errors:
                    logger.error(error)
                return spark.createDataFrame([], df.schema)

            logger.info("All data is valid. Proceeding with ingestion.")
            return valid_data

        if validation_strategy == "ingest_valid":
            valid_data, type_errors = DataValidator.validate_data_types(df, metadata_df)
            all_errors.extend(type_errors)

            valid_data, primary_key_errors = DataValidator.validate_primary_keys(valid_data, metadata_df)
            all_errors.extend(primary_key_errors)

            if all_errors:
                logger.warning(
                    "Validation errors found. Only valid data will be ingested.")
                for error in all_errors:
                    logger.warning(error)

            logger.info("Proceeding with ingestion of valid data.")
            return valid_data

        logger.error("Unknown validation strategy: %s", validation_strategy)
        return df
