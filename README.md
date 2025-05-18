# TutifyDataWarehouse

This project implements a complete data warehousing pipeline for Tutify, a virtual academy. It leverages Snowflake for storage, Apache Airflow for ETL orchestration, and Python for data cleaning.

 Project Goals
- Track student payments and teacher payouts
- Enable subject- and date-wise revenue and expense analytics
- Create a star schema for efficient querying

 Tech Stack
- Snowflake
- Apache Airflow
- Python (Pandas)
- Git, GitHub

Folder Structure
- `dags/`: Airflow DAG script for ETL
- `data/raw/`: Original CSVs
- `data/clean/`: Cleaned CSVs
- `sql/`: Snowflake SQL scripts
- `erd/`: Star schema diagram
- `reports/`: Final report and screenshots

 How to Run
1. Upload raw CSVs to `/data/raw`
2. Trigger the DAG in Airflow
3. View results in Snowflake via SQL scripts

 Author
Mashhood, IBA
