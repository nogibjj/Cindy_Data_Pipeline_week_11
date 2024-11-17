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
├── Databricks_pipeline_notebook_extract.ipynb
├── Databricks_pipeline_notebook_load_and_transform.ipynb
├── Databricks_pipeline_notebook_query.ipynb
├── Dockerfile
├── LICENSE
├── Makefile
├── README.md
├── grad-students.csv
├── main.py
├── repeat.sh
├── requirements.txt
├── setup.sh
└── test_main.py

```

## Workflow Python Script:
![image](https://github.com/user-attachments/assets/eb65b444-ae33-4070-93f7-ed53adef55b9)

![image](https://github.com/user-attachments/assets/30cd52a4-efad-48ce-9585-1b74008bb350) <br><br>

## Overview of Functions

1. **`extract_spark`**: Downloads a CSV file, loads it into a Spark DataFrame, and saves it as a Delta table for raw data storage.

2. **`load_and_transform`**: Reads the raw Delta table, adds calculated fields (e.g., `Employment_rate`), and saves the enriched data as a new Delta table.

3. **`query_delta_table`**: Executes SQL queries on the transformed Delta table to analyze and extract insights. <br><br>

## Outcome of extract, load and transform:
![image](https://github.com/user-attachments/assets/ef7f99a7-4a53-4645-8fe0-dc80e5267527) <br><br>

## Outcomes of SQL query:
```plaintext 
+----------+--------------------+--------------------+----------+-------------+---------------+----------------------+----------+------------+------------------+-------------------+
|Major_code|               Major|      Major_category|Grad_total|Grad_employed|Grad_unemployed|Grad_unemployment_rate|Grad_share|Grad_premium|   Employment_rate|Major_category_size|
+----------+--------------------+--------------------+----------+-------------+---------------+----------------------+----------+------------+------------------+-------------------+
|      2407|COMPUTER ENGINEERING|         Engineering|     82102|        73147|           1592|           0.021300793|0.36755726|  0.19753087|0.8909283574090765|              Large|
|      4000|MULTI/INTERDISCIP...|   Interdisciplinary|     14405|        12708|            261|           0.020124912|0.25991014|        0.25|0.8821936827490454|              Large|
|      2404|BIOMEDICAL ENGINE...|         Engineering|     22155|        19474|            366|           0.018447582| 0.6418576|   0.2857143|0.8789889415481833|              Large|
|      1301|ENVIRONMENTAL SCI...|Biology & Life Sc...|     43612|        38068|           1392|            0.03527623|0.31576356|  0.23636363|0.8728790241218013|              Large|
|      3611|        NEUROSCIENCE|Biology & Life Sc...|     14429|        12551|            225|           0.017611146| 0.6735599|        0.45|0.8698454501351445|              Large|
|      4101|PHYSICAL FITNESS ...|Industrial Arts &...|    119489|       103790|           2696|           0.025317881|0.28696415|  0.33333334|0.8686155210939919|              Large|
|      2105|INFORMATION SCIENCES|Computers & Mathe...|     25799|        22403|           1179|           0.049995758|0.25832322|         0.2|0.8683669909686422|              Large|
|      2102|    COMPUTER SCIENCE|Computers & Mathe...|    324402|       281088|          10557|           0.036198117| 0.3048934|      0.1875|0.8664804779255368|              Large|
|      6109|TREATMENT THERAPY...|              Health|    142285|       123279|           2185|           0.017415354|0.37737677|  0.11111111|0.8664230242119689|              Large|
|      5001|ASTRONOMY AND AST...|   Physical Sciences|      6331|         5480|            112|           0.020028612| 0.6188661|  0.12941177|0.8655820565471489|              Small|
+----------+--------------------+--------------------+----------+-------------+---------------+----------------------+----------+------------+------------------+-------------------+
```
SQL query executed successfully.
