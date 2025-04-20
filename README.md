# Contoso PoC Project

This project is a proof of concept (PoC) for data processing using PySpark and Delta Lake. It demonstrates a complete ETL (Extract, Transform, Load) process, including data creation, validation, transformation, and writing to delta format.

---

## Table of Contents

- [Contoso PoC Project](#contoso-poc-project)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Project Structure](#project-structure)
  - [Process Workflow](#process-workflow)
    - [1. **Data Creation**](#1-data-creation)
    - [2. **Data Validation**](#2-data-validation)
    - [3. **Data Transformation**](#3-data-transformation)
    - [4. **Data Writing**](#4-data-writing)
  - [Setup and Execution](#setup-and-execution)
    - [Prerequisites](#prerequisites)
    - [Steps to Run the Project](#steps-to-run-the-project)

---

## Overview

The Contoso PoC project processes financial and personal data to demonstrate the following:

- Reading raw data from CSV files.
- Validating data types and primary keys.
- Transforming and joining datasets.
- Writing the final dataset to Parquet format.

---

## Project Structure

The project is organized as follows:

poc-nn-carlos/
| src
│ ├── data_validation/
  │ ├── validator.py # Data validation logic
│ ├── utils/
  │ ├── reader_writer_config.py # File reading and writing utilities
│ ├── resources/
  │ ├── raw_zone/ # Raw input data
  │ └── datahub/ # Processed output data
  | model/
  | ├── financial_schema.csv # Financial data
  | └── personal_schema.csv # Personal data
│ test/
  │ ├── validator_test.py # Unit tests for data validation
  │ └── contoso_test.py # Integration tests for the main process
├── contoso_poc.py # Main ETL process
├── README.md # Project documentation
├── CHANGELOG.md # Change log
├── requirements.txt # Project dependencies
├── .gitignore # Git ignore file

---

## Process Workflow

### 1. **Data Creation**

- **Input Data**:
- 
  - Financial data (`financial_data.csv`): Contains `DNI`, `CreditScore`, and `Income`.
  - Personal data (`personal_data.csv`): Contains `DNI`, `FirstName`, and `Age`.
- These datasets are stored in the `resources/raw_zone/` directory.

### 2. **Data Validation**

- The `DataValidator` class in `validator.py` performs the following validations:
  - **Data Type Validation**:
    - Ensures that fields like `DNI` are integers and `FirstName` is a string.
  - **Primary Key Validation**:
    - Checks for duplicate or null values in primary key fields (e.g., `DNI`).
- Validation strategies:
  - `reject_all`: Rejects all data if any validation error is found.

### 3. **Data Transformation**

- The validated datasets are joined on the `DNI` field.
- The resulting dataset is sorted by `Age` (ascending) and `CreditScore` (descending).
- Selected fields include `FirstName`, `DNI`, `CreditScore`, and `Age`.

### 4. **Data Writing**

- The final dataset is written to Parquet format in the `resources/datahub/` directory.

---

## Setup and Execution

### Prerequisites

- Python 3.8 or higher.
- PySpark installed (`pip install pyspark`).
- A working directory with the project structure.

### Steps to Run the Project

1. Clone the repository:
   ```bash
   git clone https://github.com/charlyfdezh/PoC_Carlos.git

2. Navigate to the project directory:
   ```bash
   cd PoC_Carlos

3. Install the project dependencies:   
   ```bash
   pip install -r requirements.txt

4. Run the project:
   ```bash
   python contoso_poc.py


Changelog
Refer to the CHANGELOG.md file for a detailed list of changes and updates.

---
