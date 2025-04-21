"""
Unit and integration tests for Contoso PoC project.
"""
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.utils.reader_writer_config import Reader
from contoso_poc import main, RAW_ZONE_PATH, DATAHUB_ZONE_PATH


@pytest.fixture(scope="session")
def spark():
    """
    Fixture to initialize SparkSession.
    """
    spark = (SparkSession.builder
             .appName("TestContosoPoC")
             .master("local[*]")
             .config("spark.executor.memory", "1g")
             .config("spark.driver.memory", "1g")
             .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def setup_test_data(spark):
    """
    Fixture to create test data for financial and personal datasets using DataFrames.
    """
    # Financial data
    financial_data = [
        ("123", 750, 50000),
        ("456", 600, 30000)
    ]
    financial_schema = StructType([
        StructField("DNI", StringType(), True),
        StructField("CreditScore", IntegerType(), True),
        StructField("Income", IntegerType(), True)
    ])
    financial_df = spark.createDataFrame(financial_data, schema=financial_schema)
    financial_path = os.path.join(RAW_ZONE_PATH, "financial_data.csv")
    financial_df.write.csv(financial_path, header=True, mode="overwrite")

    # Personal data
    personal_data = [
        ("123", "John", 30),
        ("456", "Jane", 25)
    ]
    personal_schema = StructType([
        StructField("DNI", StringType(), True),
        StructField("FirstName", StringType(), True),
        StructField("Age", IntegerType(), True)
    ])
    personal_df = spark.createDataFrame(personal_data, schema=personal_schema)
    personal_path = os.path.join(RAW_ZONE_PATH, "personal_data.csv")
    personal_df.write.csv(personal_path, header=True, mode="overwrite")

    return financial_path, personal_path


def test_main_integration(spark):
    """
    Integration test for the main function in contoso_poc.py.
    """
    main()

    output_path = os.path.join(DATAHUB_ZONE_PATH, "part-*.parquet")
    output_files = Reader.read_file(spark,
                                    output_path,
                                    file_format="parquet",
                                    options={"header": "true"})

    assert output_files.count() == 100
    assert "DNI" in output_files.columns
    assert "FirstName" in output_files.columns
    assert "CreditScore" in output_files.columns
    assert "Age" in output_files.columns
