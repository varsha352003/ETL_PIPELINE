# End-to-End Stock Data ETL Pipeline

## Overview
This project is an end-to-end ETL pipeline designed to process raw stock market data and transform it into structured, analysis-ready datasets. The system performs large-scale data transformations using PySpark and stores the cleaned data in a PostgreSQL database running inside Docker.

## Architecture
Raw Data → PySpark Transformations → Parquet Files → PostgreSQL (Docker)

## Features
- Data extraction from raw stock datasets
- Data cleaning and transformation using PySpark
- Calculation of financial metrics:
  - Moving Averages
  - Daily Volatility
- Storage of processed data in Parquet format
- Loading transformed data into PostgreSQL
- Dockerized database setup for portability

## Tech Stack
- Python
- PySpark
- PostgreSQL
- Docker
- Parquet

## Workflow
1. Extract raw stock market data.
2. Perform data cleaning and transformation using PySpark.
3. Compute financial indicators such as moving averages and volatility.
4. Save processed data as Parquet files.
5. Load final datasets into PostgreSQL running in a Docker container.

## Use Cases
- Financial data analysis
- Data engineering pipeline demonstration
- Backend data processing for analytics dashboards
