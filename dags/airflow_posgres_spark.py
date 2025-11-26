from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

def spark_etl_from_csv():

    # 1. Get DB connection from Airflow
    conn = BaseHook.get_connection("postgres_conn_2")
    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"

    # 2. Start Spark
    spark = (
        SparkSession.builder
        .appName("spark_csv_to_postgres")
        .config("spark.jars", "/home/jade_lee/jars/postgresql-42.7.2.jar")
        .getOrCreate()
    )

    # 3. Read CSV
    csv_path = "/home/jade_lee/download/billing.csv"
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )

    print("Raw Data:")
    df.show(5)
    print("Number of row:", df.count())   


    # 4. Transform data (example: add discount 10%)
    df = df.dropDuplicates()

    print("Transformed Data:", df.count())

    # 5. Write to Postgres
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "billing_data")
        .option("user", conn.login)
        .option("password", conn.password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")   # append to table
        .save()
    )

    spark.stop()
    print("ETL Complete — Data written to PostgreSQL")


with DAG(
    dag_id="spark_csv_to_postgres",
    start_date=datetime(2025, 11, 4),
    schedule=None,
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id="run_spark_etl",
        python_callable=spark_etl_from_csv
    )
