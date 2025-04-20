"""
Schema for personal information table.
"""
import dataclasses
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


@dataclasses.dataclass
class PersonalSchema:
    """
    Class to define the schema for the Person table.
    """

    CUSTOMER_ID = "CustomerID"
    FIRST_NAME = "FirstName"
    LAST_NAME = "LastName"
    GENDER = "Gender"
    AGE = "Age"
    EMAIL = "Email"
    PHONE = "Phone"
    BIRTH_DATE = "BirthDate"
    DNI = "DNI"
    
    def __init__(self):
        """
        Initializes the schema for the Person table.
        """
        self.schema = StructType([
            StructField("CustomerID", IntegerType(), True),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Email", StringType(), True),
            StructField("Phone", StringType(), True),
            StructField("BirthDate", StringType(), True),
            StructField("DNI", StringType(), True),
        ])

    def get_schema(self):
        """
        Returns the schema for the Person table.
        """
        return self.schema
