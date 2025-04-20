"""
Schema for the Financial DataHub table.
"""
import dataclasses
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


@dataclasses.dataclass
class FinancialSchema:
    """
    Class to define the schema for the Financial table.
    """

    CUSTOMER_ID = "CustomerID"
    DNI = "DNI"
    ACCOUNT_BALANCE = "AccountBalance"
    CREDIT_SCORE = "CreditScore"
    LOAN_AMOUNT = "LoanAmount"
    LOAN_STATUS = "LoanStatus"
    ACCOUNT_TYPE = "AccountType"

    def __init__(self):
        """
        Initializes the schema for the Financial table.
        """
        self.schema = StructType([
            StructField("CustomerID", IntegerType(), True),
            StructField("DNI", StringType(), True),
            StructField("AccountBalance", StringType(), True),
            StructField("CreditScore", StringType(), True),
            StructField("LoanAmount", StringType(), True),
            StructField("LoanStatus", StringType(), True),
            StructField("AccountType", StringType(), True),
        ])

    def get_schema(self):
        """
        Returns the schema for the Financial table.
        """
        return self.schema
