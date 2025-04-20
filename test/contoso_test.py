"""
Unit and integration tests for Contoso PoC project.
"""
import os
import pytest
from utils.reader_writer_config import Writer
from pyspark.sql import SparkSession
from contoso_poc import main, RAW_ZONE_PATH, DATAHUB_ZONE_PATH
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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


@pytest.fixture(scope="session")
def setup_test_data(spark):
    """
    Fixture to create test data for financial and personal datasets.
    """
    financial_data = [
        {"DNI": "123", "CreditScore": 750, "Income": 50000},
        {"DNI": "456", "CreditScore": 600, "Income": 30000}
    ]
    financial_schema = StructType([
        StructField("DNI", StringType(), True),
        StructField("CreditScore", IntegerType(), True),
        StructField("Income", IntegerType(), True)
    ])
    financial_df = spark.createDataFrame(financial_data,
                                         schema=financial_schema)
    financial_path = os.path.join(RAW_ZONE_PATH, "financial_data.csv")
    financial_df.write.csv(financial_path, header=True, mode="overwrite")

    personal_data = [
        {"DNI": "123", "FirstName": "John", "Age": 30},
        {"DNI": "456", "FirstName": "Jane", "Age": 25}
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


def test_main_integration(spark, setup_test_data):
    """
    Integration test for the main function in contoso_poc.py.
    """

    main()

    output_path = os.path.join(DATAHUB_ZONE_PATH, "part-*.csv")
    output_files = spark.read.csv(output_path, header=True)

    output_files.show(truncate=False)
    assert output_files.count() == 2
    assert "DNI" in output_files.columns
    assert "FirstName" in output_files.columns
    assert "CreditScore" in output_files.columns
    assert "Age" in output_files.columns