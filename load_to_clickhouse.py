# load_to_clickhouse.py - Высокоскоростная пакетная переливка данных без переполнения ОЗУ
import os
import psycopg2
import clickhouse_connect
from datetime import datetime, timezone

def transfer_to_clickhouse():
    print("\n--- МОДУЛЬ 3.5: ПАКЕТНАЯ СИНХРОНИЗАЦИЯ СЛОЯ ВИТРИН (POSTGRESQL -> CLICKHOUSE) ---")
    
    # Синхронизация имен хостов с нашим docker_compose1.yml
    if os.path.exists('/.dockerenv') or os.getenv('AIRFLOW_CTX_DAG_ID') is not None:
        pg_host = "vkr_final_postgres"      # Имя контейнера в сети Docker
        ch_host = "vkr_final_clickhouse"    # Задел под будущий контейнер ClickHouse
    else:
        pg_host = "localhost"
        ch_host = "localhost"

    try:
        # ИБ-Оптимизация: Подключение с паролем и серверным курсором 'ch_stream_cursor'
        pg_conn = psycopg2.connect(
            host=pg_host, 
            port="5432", 
            database="vkr_art_integration_dwh", # Актуальное имя базы данных
            user="db_security_admin",
            password="strong_security_password_123" # Наш ИБ-пароль безопасности
        )
        pg_cur = pg_conn.cursor(name='ch_stream_cursor') 

        # Исправленный SQL-запрос согласно связям таблиц в model1.py
        query = """
            SELECT 
                h.lot_id_modified,
                h.record_source,
                f.price_start,
                f.price_sale,
                f.hash_diff,
                f.load_date
            FROM h_lot h
            JOIN l_auction_transaction l ON h.hk_lot = l.hk_lot
            JOIN s_lot_financials f ON l.hk_link_transaction = f.hk_link_transaction;
        """
        print(" -> Активация серверного стриминга данных из PostgreSQL...")
        pg_cur.execute(query)

         # Клиент высокоскоростного подключения к ClickHouse (Используем созданного ИБ-пользователя ВКР)
        ch_client = clickhouse_connect.get_client(
            host=ch_host, 
            port=8123, 
            username='clickhouse_analyst',        # ИСПРАВЛЕНИЕ: Точное имя пользователя из docker-compose
            password='SecureClickPass2026',       # Наш пароль безопасности
            database='art_marts_db'               # Наша целевая база данных диплома
        )

        chunk_size = 30000
        chunk_counter = 0
        
        while True:
            # Извлекаем данные из Postgres строго порциями по 30 000 строк
            rows = pg_cur.fetchmany(chunk_size)
            if not rows:
                break
                
            chunk_counter += 1
            data_tuples = []
            
            for row in rows:
                # Безопасный парсинг даты для ClickHouse
                dt_obj = row[5]
                if isinstance(dt_obj, str):
                    try:
                        dt_obj = datetime.strptime(dt_obj.split('+')[0], '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        dt_obj = datetime.now(timezone.utc)
                elif dt_obj is None:
                    dt_obj = datetime.now(timezone.utc)
                
                data_tuples.append((
                    str(row[0]) if row[0] else '',
                    str(row[1]) if row[1] else 'NOT_SPECIFIED',
                    float(row[2]) if row[2] else 0.0,
                    float(row[3]) if row[3] else 0.0,
                    str(row[4]) if row[4] else '',
                    dt_obj
                ))
            
           # Пакетный Bulk-инсерт текущей пачки в ClickHouse
            ch_client.insert(
                'art_marts_db.dm_secure_art_analytics', # Добавили префикс базы обратно
                data_tuples, 
                column_names=['lot_id', 'auction_house', 'starting_price', 'final_price', 'row_integrity_hash', 'dwh_load_timestamp']
            )
            print(f"    [+] Пакет №{chunk_counter} ({len(data_tuples)} строк) успешно перелит в ClickHouse.")
            
        print(" [✓] СЛОЙ ВИТРИН В CLICKHOUSE УСПЕШНО ОБНОВЛЕН БЕЗ ПЕРЕГРУЗКИ ОЗУ.")
        pg_cur.close()
        pg_conn.close()
        
    except Exception as e:
        print(f" [!] Ошибка порционной трансляции данных: {e}")

if __name__ == "__main__":
    transfer_to_clickhouse()