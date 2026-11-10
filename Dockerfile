# Tayyor Airflow image'ini asos qilib olamiz
FROM apache/airflow:2.9.3-python3.11

USER airflow

# dbt'ni alohida "virtual muhit"ga o'rnatamiz,
# shunda u Airflow kutubxonalari bilan to'qnashmaydi
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres==1.8.2