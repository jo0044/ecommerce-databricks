# Ecommerce Databricks Project

## Overview
An end-to-end data engineering project built using Databricks, PySpark, SQL and Delta Lake. The project processes e-commerce data using the Medallion Architecture (Bronze, Silver and Gold layers).

## Technologies
- Databricks
- PySpark
- SQL
- Python
- Delta Lake

## Data Processing

**1. Dimension Processing**
- Processed Customers, Products, Brands, Categories and Calendar datasets.
- Loaded raw CSV files into Bronze Delta tables.
- Cleaned and transformed data in Silver.
- Enriched datasets in Gold using joins and additional attributes.

**2. Fact Processing**
- Processed Orders and Order Items datasets.
- Loaded and transformed transactional data through Bronze, Silver and Gold.
- Added calculated business fields to prepare datasets for analysis.

## Architecture

- **Bronze:** Raw data ingestion.
- **Silver:** Data cleaning and transformation.
- **Gold:** Enriched, business-ready datasets.

## Project Structure

```text
ecommerce/
├── 1_setup/
├── 2_medallion_processing_dim/
└── 3_medallion_processing_fact/
```

## Key Learnings
- Building ETL workflows with PySpark and SQL.
- Working with Delta tables and Medallion Architecture.
- Data cleaning, transformation and validation.
- Dimension and fact data processing.
- Preparing datasets for analytical reporting.
