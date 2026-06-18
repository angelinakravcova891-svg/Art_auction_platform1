from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Базовые аргументы оркестрации для ВКР
default_args = {
    "owner": "student_vkr", 
    "start_date": datetime(2026, 1, 1), 
    "retries": 0
}

with DAG(
    "vkr_secure_art_platform", 
    default_args=default_args, 
    description="Platform Data Pipeline", 
    schedule_interval=None, 
    catchup=False, 
    tags=["vkr_security", "data_vault"]
) as dag:

    # Шаг 1: Сбор данных
    task_1_fetch = BashOperator(
        task_id="1_fetch_raw_data", 
        bash_command="echo '2' | python3 /opt/airflow/src/1_fetcher.py"
    )

    # Шаг 2: Очистка данных и генерация подписи безопасности
    task_2_clean = BashOperator(
        task_id="2_clean_and_mask_data", 
        bash_command="python3 /opt/airflow/src/2_cleaner.py"
    )

    # Шаг 3: Импорт в PostgreSQL (Data Vault 2.0)
    task_3_load_vault = BashOperator(
        task_id="3_load_to_data_vault", 
        bash_command="python3 /opt/airflow/src/etl1.py"
    )

    # Шаг 3.5: Переливка Big Data витрин в ClickHouse с автоматической установкой драйвера внутри Docker
    task_3_5_load_clickhouse = BashOperator(
        task_id="3_5_load_to_clickhouse", 
        # ИСПРАВЛЕНИЕ: сначала ставим пакет в контейнер, затем запускаем скрипт
        bash_command="pip install --user clickhouse-connect && python3 /opt/airflow/src/load_to_clickhouse.py"
    )

    # Шаг 4: ИБ-сканер целостности файлов на диске
    task_4_analyze = BashOperator(
        task_id="4_security_integrity_scan", 
        bash_command="python3 /opt/airflow/src/4_attack_simulator.py"
    )

    # Строим последовательность выполнения графа:
    task_1_fetch >> task_2_clean >> task_3_load_vault >> task_3_5_load_clickhouse >> task_4_analyze
