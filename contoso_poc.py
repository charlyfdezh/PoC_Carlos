"""
Contoso PoC for Nationale-Nederlanden with pyspark and Delta Lake
"""
import os
import logging
import sys
import pyspark 
import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, desc
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
from utils.logger_config import LoggerConfig
from utils.reader_writer_config import Reader, Writer
from data_validation.validator import DataValidator
from model.financial_schema import FinancialSchema as F
from model.personal_schema import PersonalSchema as P

logger = LoggerConfig.configure_logger(log_file_path="application.log",
                                       log_level=logging.INFO)

BASE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "resources"))
RAW_ZONE_PATH = os.path.join(BASE_PATH, "raw_zone")
DATAHUB_ZONE_PATH = os.path.join(BASE_PATH, "datahub_zone")
financial_path = os.path.join(RAW_ZONE_PATH, "financial_data.csv")
personal_path = os.path.join(RAW_ZONE_PATH, "personal_data.csv")
financial_metadata_path = os.path.join(RAW_ZONE_PATH, "financial_metadata.csv")
personal_metadata_path = os.path.join(RAW_ZONE_PATH, "personal_metadata.csv")
output_path = os.path.join(DATAHUB_ZONE_PATH)

# Configuring Azure Key Vault
# key_vault_name = "<contoso-key-vault-name>"
# key_vault_url = f"https://{key_vault_name}.vault.azure.net/"
# credential = DefaultAzureCredential()
# client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieving connection string from Azure Key Vault
# (Assuming connection string is stored as a secret in Key Vault)
# secret_name = "OLAPConnectionString"
# connection_string = client.get_secret(secret_name).value


def main():
    """
    Main function to execute the ETL process.
    """
    spark = (SparkSession.builder
             .appName("ContosoPoC")
             .master("local[*]")
             .config("spark.executor.memory", "2g")
             .config("spark.driver.memory", "2g")
             .config("spark.hadoop.hadoop.native.lib", "false")
             .getOrCreate())
    
    #  .config("spark.sql.extensions",
    #          "io.delta.sql.DeltaSparkSessionExtension")
    #  .config("spark.sql.catalog.spark_catalog",
    #          "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    logger.info("Reading financial data.")
    financial_df = Reader.read_file(spark,
                                    financial_path,
                                    file_format="csv",
                                    options={"header": "true"},
                                    schema=F().get_schema())

    logger.info("Reading personal data.")
    personal_df = Reader.read_file(spark,
                                   personal_path,
                                   file_format="csv",
                                   options={"header": "true"},
                                   schema=P().get_schema())

    # OLAP Extraction
    # olap_df = Reader.read_olap(spark,
    #                            connection_string=connection_string,
    #                            query="SELECT * FROM table")

    # Data Validation
    validated_financial_data = DataValidator.validate_data(
        spark=spark, df=financial_df, metadata_path=financial_metadata_path
    )

    # Data Transformation
    combined_df = (validated_financial_data.join(personal_df,
                                                 on="DNI", how="inner")
                   .sort(asc("Age"), desc("CreditScore"))
                   .select(P.FIRST_NAME, P.DNI, F.CREDIT_SCORE, P.AGE))

    # Data Loading. Saving to Delta Lake
    Writer.write_file(spark, combined_df, output_path)

    logger.info("Contoso PoC completed successfully.")

    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()

