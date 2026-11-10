"""Kechagi buyurtmalarni PostgreSQL'dan staging jadvaliga yuklaydi (idempotent),
keyin dbt orqali kunlik daromad modelini quradi va testlarni ishga tushiradi."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project"
# dbt log/target fayllari mount qilingan papkaga emas, vaqtinchalik joyga yoziladi
DBT_ENV = {"DBT_LOG_PATH": "/tmp/dbt_logs", "DBT_TARGET_PATH": "/tmp/dbt_target"}

CREATE_STAGING = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id    INT PRIMARY KEY,
    customer_id INT,
    amount      NUMERIC(12,2),
    status      TEXT,
    created_at  TIMESTAMP,
    order_date  DATE NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now()
);
"""


def load_orders(target_date: str) -> int:
    """target_date (YYYY-MM-DD) kunidagi buyurtmalarni yuklaydi.

    Idempotent: shu kunning eski qatorlari o'chiriladi va qayta yoziladi,
    ikkalasi ham bitta tranzaksiyada. Necha marta ishga tushirilsa ham natija bir xil.
    """
    src = PostgresHook(postgres_conn_id="source_pg")
    rows = src.get_records(
        """
        SELECT order_id, customer_id, amount, status, created_at, created_at::date
        FROM orders
        WHERE created_at >= %(d)s::date AND created_at < %(d)s::date + 1
        """,
        parameters={"d": target_date},
    )

    wh = PostgresHook(postgres_conn_id="warehouse_pg")
    conn = wh.get_conn()
    try:
        with conn, conn.cursor() as cur:  # `with conn` = commit yoki rollback
            cur.execute(CREATE_STAGING)
            cur.execute("DELETE FROM staging.orders WHERE order_date = %s", (target_date,))
            if rows:
                execute_values(
                    cur,
                    """INSERT INTO staging.orders
                       (order_id, customer_id, amount, status, created_at, order_date)
                       VALUES %s""",
                    rows,
                )
    finally:
        conn.close()
    print(f"{target_date}: {len(rows)} ta buyurtma yuklandi")
    return len(rows)


with DAG(
    dag_id="orders_daily_elt",
    description="Orders -> staging (idempotent) -> dbt daily_revenue + tests",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["orders", "dbt"],
) as dag:
    # data_interval_start = "kecha" (qo'lda ishga tushirilganda ham)
    load_staging = PythonOperator(
        task_id="load_yesterdays_orders",
        python_callable=load_orders,
        op_kwargs={"target_date": "{{ data_interval_start | ds }}"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run {DBT_ARGS}",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_ARGS}",
        env=DBT_ENV,
        append_env=True,
    )

    load_staging >> dbt_run >> dbt_test
