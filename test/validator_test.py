"""
Unit test for the DataValidator class.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
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


@pytest.fixture(params=[
    (
        [
            {"CustomerID": "123", "Name": "John Doe", "Age": "30"},
            {"CustomerID": "456", "Name": "Jane Smith", "Age": "25"}
        ],
        [
            {"field_name": "CustomerID", "field_type": "Integer", "key": "K1"},
            {"field_name": "Name", "field_type": "String", "key": None},
            {"field_name": "Age", "field_type": "Integer", "key": None}
        ]
    )
])
def sample_data_with_params(request, spark):
    """
    Fixture parametrizado para crear diferentes combinaciones de datos y metadatos.
    """
    data, metadata = request.param

    schema = StructType([
        StructField("CustomerID", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Age", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)

    metadata_schema = StructType([
        StructField("field_name", StringType(), True),
        StructField("field_type", StringType(), True),
        StructField("key", StringType(), True)
    ])
    metadata_df = spark.createDataFrame(metadata, metadata_schema)

    return df, metadata_df


def test_validate_data_types(sample_data_with_params):
    """
    Unit test for validating data types.
    """
    df, metadata_df = sample_data_with_params

    valid_data, errors = DataValidator.validate_data_types(df, metadata_df)

    assert valid_data.count() == 1
    assert len(errors) == 2


def test_validate_primary_keys(sample_data):
    """
    Unit test for validating primary keys.
    """
    df, metadata_df = sample_data

    valid_data, errors = DataValidator.validate_primary_keys(df, metadata_df)

    assert valid_data.count() == 1
    assert len(errors) == 2


def test_validate_data(sample_data):
    """
    Unit test for validating complete data
    """

    valid_data = DataValidator.validate_data(spark, sample_data, metadata_path=None)

    assert valid_data.count() == 0
