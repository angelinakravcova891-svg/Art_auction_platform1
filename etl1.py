# etl1.py - Пакетный конвейер Data Vault 2.0 
import os
import csv
import time
import hashlib
import psycopg2
import pandas as pd
from datetime import datetime, timezone
from psycopg2.extras import execute_values

def get_db_connection():
    """Проверка окружения: Docker / Airflow или локальный запуск с Mac."""
    if os.path.exists('/.dockerenv') or os.getenv('AIRFLOW_CTX_DAG_ID') is not None:
        db_host = "vkr_final_postgres"  # Внутри сети Docker-контейнеров
    else:
        db_host = "localhost"           # При запуске напрямую из терминала Mac

    return psycopg2.connect(
        host=db_host,
        port="5432",
        database="vkr_art_integration_dwh",
        user="db_security_admin",
        password="strong_security_password_123"  # ИСПРАВЛЕНИЕ: Добавляем пароль для всех типов подключений
    )

def init_database():
    """Мгновенно очищает буферную таблицу и активирует ИБ-модуль pgcrypto."""
    print(" -> [БЫСТРЫЙ СТАРТ] Очистка буфера и подготовка таблиц...")
    try:
        # ИСПРАВЛЕНИЕ: Передаем параметры подключения явно прямо в функцию
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="vkr_art_integration_dwh",
            user="db_security_admin",
            password="strong_security_password_123" # Жестко передаем пароль для ИБ
        )
        cur = conn.cursor()
        
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        cur.execute("TRUNCATE TABLE stg_auction_data RESTART IDENTITY CASCADE;")
        
        conn.commit()
        cur.close()
        conn.close()
        print(" -> [ОК] Буфер Big Data успешно очищен. База готова к приему.")
    except Exception as e:
        print(f" !!! Ошибка очистки СУБД: {e}")
def load_staging_file(file_path):
    """Высокоскоростной пакетный импорт в буфер и распределение по Data Vault 2.0."""
    print(f" -> Запуск ИБ-оптимизированного импорта в буфер данных: {file_path}")
    
    # Мы проверили через терминал, что файл есть, поэтому убираем os.path.exists
    # Это принудительно заставит Pandas начать чтение потока напрямую с диска Mac

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        print(" -> Очистка Staging таблицы...")
        cur.execute("TRUNCATE TABLE stg_auction_data;")
        conn.commit()

        db_columns = [
            'lot_id_modified', 'reg_num_lota_mod', 'god_provedeniya', 'kategoriya_arta',
            'naimenovanie_lota_mod', 'period', 'artist_id_raw', 'author_name_natural',
            'nationality', 'material_tech', 'gabariti', 'auction_house_id',
            'auction_house', 'classification', 'city_session', 'price_start', 'price_sale',
            'buyer_id_token', 'seller_id_token', 'load_date', 'record_source'
        ]
        
        query = f"INSERT INTO stg_auction_data ({', '.join(db_columns)}) VALUES %s"
        
        chunk_size = 50000  # Размер пачки для стабильности памяти Mac
        chunk_counter = 0
        total_rows_loaded = 0
        start_time = time.time()
        
        print(" -> Начинаем чтение тяжелого файла пачками...")
        for chunk in pd.read_csv(file_path, dtype=str, chunksize=chunk_size, encoding='utf-8'):
            chunk_counter += 1
            chunk = chunk.fillna('').convert_dtypes()
            chunk.columns = chunk.columns.str.lower()
            
            chunk['price_start'] = pd.to_numeric(chunk['price_start'], errors='coerce').fillna(0.00)
            chunk['price_sale'] = pd.to_numeric(chunk['price_sale'], errors='coerce').fillna(0.00)
            chunk['god_provedeniya'] = pd.to_numeric(chunk['god_provedeniya'], errors='coerce').fillna(0).astype(int)
            
            if 'period' in chunk.columns:
                chunk['period'] = chunk['period'].str.extract(r'(\d{4})').fillna(0).astype(int)
            
            if 'load_date' not in chunk.columns or chunk['load_date'].eq('').all():
                chunk['load_date'] = datetime.now(timezone.utc).isoformat()
            
            for col in db_columns:
                if col not in chunk.columns:
                    chunk[col] = ''
            
            df_chunk = chunk[db_columns]
            data_tuples = [tuple(x) for x in df_chunk.to_numpy()]
            
            execute_values(cur, query, data_tuples)
            conn.commit()
            
            total_rows_loaded += len(data_tuples)
            elapsed = time.time() - start_time
            print(f"    [PROGRESS] Загружено строк: {total_rows_loaded:,} | Пачка №{chunk_counter} | Прошло времени: {elapsed:.1f} сек.")

        # --- ОТСЧЕТ ДЛЯ ХАБОВ ---
        print("\n -> [1/3] Заполнение Хабов (Расчет SHA-256 ключей бизнес-сущностей)...")
        t0 = time.time()
        
        cur.execute("""
            INSERT INTO h_lot (hk_lot, lot_id_modified, load_date, record_source)
            SELECT DISTINCT encode(digest(lot_id_modified, 'sha256'), 'hex'), lot_id_modified, CAST(load_date AS TIMESTAMP WITH TIME ZONE), record_source
            FROM stg_auction_data WHERE lot_id_modified != '' ON CONFLICT (hk_lot) DO NOTHING;
        """)
        print(f"      [+] Хаб Лотов обработан за {time.time()-t0:.1f} сек.")
        
        cur.execute("""
            INSERT INTO h_buyer (hk_buyer, buyer_id_token, load_date, record_source)
            SELECT DISTINCT encode(digest(buyer_id_token, 'sha256'), 'hex'), buyer_id_token, CAST(load_date AS TIMESTAMP WITH TIME ZONE), record_source
            FROM stg_auction_data WHERE buyer_id_token != '' ON CONFLICT (hk_buyer) DO NOTHING;
        """)
        cur.execute("""
            INSERT INTO h_seller (hk_seller, seller_id_token, load_date, record_source)
            SELECT DISTINCT encode(digest(seller_id_token, 'sha256'), 'hex'), seller_id_token, CAST(load_date AS TIMESTAMP WITH TIME ZONE), record_source
            FROM stg_auction_data WHERE seller_id_token != '' ON CONFLICT (hk_seller) DO NOTHING;
        """)
        cur.execute("""
            INSERT INTO h_auction_house (hk_auction_house, auction_house_id, load_date, record_source)
            SELECT DISTINCT encode(digest(auction_house_id, 'sha256'), 'hex'), auction_house_id, CAST(load_date AS TIMESTAMP WITH TIME ZONE), record_source
            FROM stg_auction_data WHERE auction_house_id != '' ON CONFLICT (hk_auction_house) DO NOTHING;
        """)
        cur.execute("""
            INSERT INTO h_geography (hk_geography, city_session, load_date, record_source)
            SELECT DISTINCT encode(digest(city_session, 'sha256'), 'hex'), city_session, CAST(load_date AS TIMESTAMP WITH TIME ZONE), record_source
            FROM stg_auction_data WHERE city_session != '' ON CONFLICT (hk_geography) DO NOTHING;
        """)
        conn.commit()
        # --- ОТСЧЕТ ДЛЯ ЛИНКОВ ---
        print("\n -> [2/3] Синхронизация транзакций: Заполнение слоя связей Линков...")
        t0 = time.time()
        cur.execute("""
            INSERT INTO l_auction_transaction (hk_link_transaction, hk_lot, hk_buyer, hk_seller, hk_auction_house, hk_geography, load_date, record_source)
            SELECT DISTINCT 
                encode(digest(lot_id_modified || coalesce(buyer_id_token, '') || coalesce(seller_id_token, '') || coalesce(auction_house_id, ''), 'sha256'), 'hex'),
                encode(digest(lot_id_modified, 'sha256'), 'hex'),
                encode(digest(coalesce(buyer_id_token, 'SYSTEM_EMPTY'), 'sha256'), 'hex'),
                encode(digest(coalesce(seller_id_token, 'SYSTEM_EMPTY'), 'sha256'), 'hex'),
                encode(digest(coalesce(auction_house_id, 'SYSTEM_EMPTY'), 'sha256'), 'hex'),
                encode(digest(coalesce(city_session, 'SYSTEM_EMPTY'), 'sha256'), 'hex'),
                CAST(load_date AS TIMESTAMP WITH TIME ZONE), record_source
            FROM stg_auction_data 
            WHERE lot_id_modified != ''
            ON CONFLICT (hk_link_transaction) DO NOTHING;
        """)
        conn.commit()
        print(f"      [+] Линки транзакций связаны за {time.time()-t0:.1f} сек.")
        
        # --- ОТСЧЕТ ДЛЯ САТЕЛЛИТОВ ---
        print("\n -> [3/3] Фиксация изменений: Заполнение слоя Сателлитов (Метрики и Контекст)...")
        
        # Финансы
        t0 = time.time()
        cur.execute("""
            INSERT INTO s_lot_financials (hk_link_transaction, load_date, price_start, price_sale, hash_diff, record_source)
            SELECT 
                encode(digest(lot_id_modified || coalesce(buyer_id_token, '') || coalesce(seller_id_token, '') || coalesce(auction_house_id, ''), 'sha256'), 'hex'),
                CAST(load_date AS TIMESTAMP WITH TIME ZONE), 
                price_start::numeric, 
                price_sale::numeric,
                encode(digest(price_start::text || price_sale::text, 'sha256'), 'hex'), 
                record_source
            FROM stg_auction_data
            WHERE lot_id_modified != ''
            ON CONFLICT (hk_link_transaction, load_date) DO NOTHING;
        """)
        conn.commit()
        print(f"      [+] Сателлит финансов (s_lot_financials) обновлен за {time.time()-t0:.1f} сек.")
        
         # Контекст описания
        t0 = time.time()
        cur.execute("""
            INSERT INTO s_lot_passive (hk_lot, load_date, naimenovanie_lota_mod, reg_num_lota_mod, god_provedeniya, kul_hist_period, hash_diff, record_source)
            SELECT 
                encode(digest(lot_id_modified, 'sha256'), 'hex'),
                CAST(load_date AS TIMESTAMP WITH TIME ZONE), 
                naimenovanie_lota_mod,
                reg_num_lota_mod,
                god_provedeniya::smallint,
                period::varchar, 
                encode(digest(coalesce(naimenovanie_lota_mod, '') || coalesce(reg_num_lota_mod, '') || god_provedeniya::text || period::text, 'sha256'), 'hex'),
                record_source
            FROM stg_auction_data
            WHERE lot_id_modified != ''
            ON CONFLICT (hk_lot, load_date) DO NOTHING;
        """)
        conn.commit()
        print(f"      [+] Сателлит описания лотов (s_lot_passive) обновлен за {time.time()-t0:.1f} сек.")
        
        # География проведения (СВЯЗКА: ГОРОД АУКЦИОНА ➔ СТРАНА ПРОВЕДЕНИЯ В КОДАХ ISO CHAR(3))
        t0 = time.time()
        cur.execute("""
            INSERT INTO s_geography_details (hk_geography, load_date, base_currency, regional_vat_rate, hash_diff, record_source)
            SELECT 
                encode(digest(city_session, 'sha256'), 'hex'),
                CAST(load_date AS TIMESTAMP WITH TIME ZONE), 
                -- ИСПРАВЛЕНИЕ: Переводим страны в коды ISO CHAR(3), чтобы не ломать DDL из model1.py
                CASE 
                    WHEN city_session = 'London' THEN 'GBR'
                    WHEN city_session = 'New York' THEN 'USA'
                    WHEN city_session = 'Paris' THEN 'FRA'
                    WHEN city_session = 'Berlin' THEN 'DEU'
                    WHEN city_session = 'Hong Kong' THEN 'CHN'
                    WHEN city_session = 'Vienna' THEN 'AUT'
                    WHEN city_session = 'Geneva' THEN 'CHE'
                    WHEN city_session = 'Milan' THEN 'ITA'
                    ELSE 'INT'
                END AS base_currency,
                20.00, -- Базовая налоговая ставка (VAT) для аналитики
                encode(digest(city_session, 'sha256'), 'hex'), 
                record_source
            FROM stg_auction_data
            WHERE city_session != ''
            ON CONFLICT (hk_geography, load_date) DO NOTHING;
        """)
        conn.commit()
        print(f"      [+] Географический сателлит стран торгов (s_geography_details) успешно обновлен за {time.time()-t0:.1f} сек.")
        
        print(f"\n [✓] КОНВЕЙЕР ИМПОРТА УСПЕШНО ЗАВЕРШЕН. Всего обработано: {total_rows_loaded:,} строк за {time.time()-start_time:.1f} сек.")

    except Exception as e:
        print(f" [!] ОШИБКА трансляции данных: {e}")
        if 'conn' in locals(): 
            conn.rollback()
    finally:
        if 'cur' in locals() and cur is not None: 
            cur.close()
        if 'conn' in locals() and conn is not None: 
            conn.close()

def run_etl_pipeline():
    """Точка вызова конвейера для интеграции с Apache Airflow DAG."""
    init_database()
    
    # ИСПРАВЛЕНИЕ: Python сам находит папку, где лежит запущенный etl1.py,
    # автоматически учитывая любые скрытые пробелы в названии папок на Mac!
    current_folder = os.path.dirname(os.path.abspath(__file__))
    
    # Собираем правильный относительный путь
    relative_file_path = os.path.join(current_folder, "data_lake_clean", "real_live_prepared_data.csv")
    
    # Запускаем импорт
    load_staging_file(relative_file_path)

if __name__ == "__main__":
    run_etl_pipeline()
