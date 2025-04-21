"""
Unit test for the DataValidator class.
"""
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from contoso_poc import RAW_ZONE_PATH
from src.data_validation.validator import DataValidator


@pytest.fixture(scope="session")
def spark():
    """
    Fixture to initialize SparkSession.
    """
    spark = (SparkSession.builder
             .appName("TestValidator")
             .master("local[*]")
             .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture(scope="function", params=[
    (
        [
            (123, "John Doe", 30),
            (456, "Jane Smith", 25)
        ],
        [
            ("CustomerID", "Integer", "K1"),
            ("Name", "String", None),
            ("Age", "Integer", None)
        ],
        2,  # Expected valid rows
        0   # Expected errors
    ),
])
def sample_data(request, spark):
    """
    Parametrized fixture to create test data and metadata.
    """
    data, metadata, expected_valid_rows, expected_errors = request.param

    data_schema = StructType([
        StructField("CustomerID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, data_schema)

    metadata_schema = StructType([
        StructField("field_name", StringType(), True),
        StructField("field_type", StringType(), True),
        StructField("key", StringType(), True)
    ])
    metadata_df = spark.createDataFrame(metadata, metadata_schema)

    return df, metadata_df, expected_valid_rows, expected_errors


def test_validate_data_types(sample_data):
    """
    Unit test for validating data types.
    """
    df, metadata_df, expected_valid_rows, expected_errors = sample_data

    valid_data, errors = DataValidator.validate_data_types(df, metadata_df)

    assert valid_data.count() == expected_valid_rows
    assert len(errors) == expected_errors


def test_validate_primary_keys(sample_data):
    """
    Unit test for validating primary keys.
    """
    df, metadata_df, expected_valid_rows, expected_errors = sample_data

    valid_data, errors = DataValidator.validate_primary_keys(df, metadata_df)

    if expected_errors > 0 and "Duplicate primary key" in str(errors):
        assert valid_data.count() == 0
    else:
        assert valid_data.count() == expected_valid_rows


def test_validate_data(spark):
    """
    Integration test for the validate_data method in DataValidator.
    """
    data = [
        (123, "John Doe", 30),
        (456, "Jane Smith", 25),
    ]
    schema = StructType([
        StructField("CustomerID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True)
    ])
    df = spark.createDataFrame(data, schema)

    metadata_data = [
        ("CustomerID", "Integer", "K1"),
        ("Name", "String", None),
        ("Age", "Integer", None)
    ]
    metadata_schema = StructType([
        StructField("field_name", StringType(), True),
        StructField("field_type", StringType(), True),
        StructField("key", StringType(), True)
    ])
    metadata_df = spark.createDataFrame(metadata_data, metadata_schema)

    metadata_path = os.path.join(RAW_ZONE_PATH, "test_data/metadata.csv")
    metadata_df.write.csv(metadata_path, header=True, mode="overwrite")

    result_df = DataValidator.validate_data(
        spark, df, metadata_path
    )
    assert result_df.count() == 3
    assert "CustomerID" in result_df.columns
    assert "Name" in result_df.columns
    assert "Age" in result_df.columns
