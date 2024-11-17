[![CI](https://github.com/nogibjj/Cindy_Data_Pipeline_week_11/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Cindy_Data_Pipeline_week_11/actions/workflows/cicd.yml)
# Week 11: Data Pipeline with Databricks

## Project Overview:
This project implements a data pipeline using Apache Spark with Delta Lake for data processing. The pipeline extracts, transforms, and queries data, following modern data engineering best practices. It includes CI/CD integration for testing and code quality checks. <br><br>

## Raw data Souce: 
https://raw.githubusercontent.com/fivethirtyeight/data/master/college-majors/grad-students.csv <br><br>

## Project Structure:
```plaintext 
Cindy_Data_Pipeline_week_11/
│
├── .devcontainer/
│
├── .github/
│   └── workflows/
│       └── cicd.yml
│
├── mylib/
│   ├── __init__.py
│   ├── extract.py
│   ├── load_and_transform.py
│   └── sql_query.py
│
├── .gitignore
├── Dockerfile
├── LICENSE
├── Makefile
├── README.md
├── main.py
├── repeat.sh
├── requirements.txt
├── setup.sh
└── test_main.py
```

## Workflow Python Script:
![image](https://github.com/user-attachments/assets/eb65b444-ae33-4070-93f7-ed53adef55b9)

![image](https://github.com/user-attachments/assets/30cd52a4-efad-48ce-9585-1b74008bb350)

## Overview of Functions

1. **`extract_spark`**: Downloads a CSV file, loads it into a Spark DataFrame, and saves it as a Delta table for raw data storage.

2. **`load_and_transform`**: Reads the raw Delta table, adds calculated fields (e.g., `Employment_rate`), and saves the enriched data as a new Delta table.

3. **`query_delta_table`**: Executes SQL queries on the transformed Delta table to analyze and extract insights.


