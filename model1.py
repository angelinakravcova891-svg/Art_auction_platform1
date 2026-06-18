# model1.py - Слой описания метаданных и DDL структуры Data Vault 2.0 с ИБ-оптимизацией
import os
import psycopg2

def get_db_connection():
    """Централизованное ИБ-подключение к единой СУБД проекта."""
    if os.path.exists('/.dockerenv') or os.getenv('AIRFLOW_CTX_DAG_ID') is not None:
        db_host = "vkr_final_postgres" # Внутри сети Docker
    else:
        db_host = "localhost"          # Локальный хост

    return psycopg2.connect(
        host=db_host,
        port="5432",
        database="vkr_art_integration_dwh", # Новое название базы данных
        user="db_security_admin",
        password="strong_security_password_123"
    )

# Активация криптографического модуля СУБД для нативного расчета SHA-256
INIT_CRYPTO_EXTENSION = "CREATE EXTENSION IF NOT EXISTS pgcrypto;"

DROP_TABLES = """
DROP TABLE IF EXISTS s_lot_financials CASCADE;
DROP TABLE IF EXISTS s_geography_details CASCADE;
DROP TABLE IF EXISTS s_category_details CASCADE;
DROP TABLE IF EXISTS s_auction_house CASCADE;
DROP TABLE IF EXISTS s_seller_details CASCADE;
DROP TABLE IF EXISTS s_buyer_details CASCADE;
DROP TABLE IF EXISTS s_artist_details CASCADE;
DROP TABLE IF EXISTS s_lot_physical CASCADE;
DROP TABLE IF EXISTS s_lot_passive CASCADE;
DROP TABLE IF EXISTS l_lot_category CASCADE;
DROP TABLE IF EXISTS l_auction_transaction CASCADE;
DROP TABLE IF EXISTS h_geography CASCADE;
DROP TABLE IF EXISTS h_category CASCADE;
DROP TABLE IF EXISTS h_auction_house CASCADE;
DROP TABLE IF EXISTS h_artist CASCADE;
DROP TABLE IF EXISTS h_seller CASCADE;
DROP TABLE IF EXISTS h_buyer CASCADE;
DROP TABLE IF EXISTS h_lot CASCADE;
DROP TABLE IF EXISTS stg_auction_data CASCADE;
"""

# 1. Оптимизированный Слой Staging (Буфер Big Data)
CREATE_STAGING_TABLE = """
CREATE TABLE stg_auction_data (
    lot_id_modified VARCHAR(64),
    reg_num_lota_mod VARCHAR(64),          
    god_provedeniya SMALLINT,              
    kategoriya_arta VARCHAR(64),           
    naimenovanie_lota_mod VARCHAR(256),    
    period VARCHAR(128),                   
    artist_id_raw VARCHAR(32),             
    author_name_natural VARCHAR(128),      
    nationality VARCHAR(64),               
    material_tech VARCHAR(128),            
    gabariti VARCHAR(64),                  
    auction_house_id VARCHAR(32),          
    auction_house VARCHAR(64),             
    classification VARCHAR(64),            
    city_session VARCHAR(64),              
    price_start NUMERIC(12, 2),            
    price_sale NUMERIC(12, 2),             
    buyer_id_token VARCHAR(128),
    seller_id_token VARCHAR(128),
    load_date TIMESTAMP WITH TIME ZONE,    
    record_source VARCHAR(64)              
);
"""

# 2. Оптимизированный Слой ХАБОВ (7 таблиц)
CREATE_HUBS = [
    "CREATE TABLE h_lot (hk_lot CHAR(64) PRIMARY KEY, lot_id_modified VARCHAR(64) NOT NULL, load_date TIMESTAMP WITH TIME ZONE NOT NULL, record_source VARCHAR(64) NOT NULL);",
    "CREATE TABLE h_buyer (hk_buyer CHAR(64) PRIMARY KEY, buyer_id_token VARCHAR(128) NOT NULL, load_date TIMESTAMP WITH TIME ZONE NOT NULL, record_source VARCHAR(64) NOT NULL);",
    "CREATE TABLE h_seller (hk_seller CHAR(64) PRIMARY KEY, seller_id_token VARCHAR(128) NOT NULL, load_date TIMESTAMP WITH TIME ZONE NOT NULL, record_source VARCHAR(64) NOT NULL);",
    "CREATE TABLE h_artist (hk_artist CHAR(64) PRIMARY KEY, artist_id VARCHAR(32) NOT NULL, load_date TIMESTAMP WITH TIME ZONE NOT NULL, record_source VARCHAR(64) NOT NULL);",
    "CREATE TABLE h_auction_house (hk_auction_house CHAR(64) PRIMARY KEY, auction_house_id VARCHAR(32) NOT NULL, load_date TIMESTAMP WITH TIME ZONE NOT NULL, record_source VARCHAR(64) NOT NULL);",
    "CREATE TABLE h_category (hk_category CHAR(64) PRIMARY KEY, kategoriya_arta VARCHAR(64) NOT NULL, load_date TIMESTAMP WITH TIME ZONE NOT NULL, record_source VARCHAR(64) NOT NULL);",
    "CREATE TABLE h_geography (hk_geography CHAR(64) PRIMARY KEY, city_session VARCHAR(64) NOT NULL, load_date TIMESTAMP WITH TIME ZONE NOT NULL, record_source VARCHAR(64) NOT NULL);"
]

# 3. Оптимизированный Слой ЛИНКОВ (2 таблицы)
CREATE_LINKS = [
    """
    CREATE TABLE l_auction_transaction (
        hk_link_transaction CHAR(64) PRIMARY KEY,
        hk_lot CHAR(64) REFERENCES h_lot(hk_lot),
        hk_buyer CHAR(64) REFERENCES h_buyer(hk_buyer),
        hk_seller CHAR(64) REFERENCES h_seller(hk_seller),
        hk_auction_house CHAR(64) REFERENCES h_auction_house(hk_auction_house),
        hk_geography CHAR(64) REFERENCES h_geography(hk_geography),
        load_date TIMESTAMP WITH TIME ZONE NOT NULL,
        record_source VARCHAR(64) NOT NULL
    );
    """,
    """
    CREATE TABLE l_lot_category (
        hk_link_lot_category CHAR(64) PRIMARY KEY,
        hk_lot CHAR(64) REFERENCES h_lot(hk_lot),
        hk_category CHAR(64) REFERENCES h_category(hk_category),
        load_date TIMESTAMP WITH TIME ZONE NOT NULL,
        record_source VARCHAR(64) NOT NULL
    );
    """
]

# 4. Оптимизированный Слой САТЕЛЛИТОВ (9 таблиц)
CREATE_SATELLITES = [
    "CREATE TABLE s_lot_passive (hk_lot CHAR(64) REFERENCES h_lot(hk_lot), load_date TIMESTAMP WITH TIME ZONE, naimenovanie_lota_mod VARCHAR(256), reg_num_lota_mod VARCHAR(64), god_provedeniya SMALLINT, kul_hist_period VARCHAR(128), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_lot, load_date));",
    "CREATE TABLE s_lot_physical (hk_lot CHAR(64) REFERENCES h_lot(hk_lot), load_date TIMESTAMP WITH TIME ZONE, material_tech VARCHAR(128), gabariti VARCHAR(64), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_lot, load_date));",
    "CREATE TABLE s_artist_details (hk_artist CHAR(64) REFERENCES h_artist(hk_artist), load_date TIMESTAMP WITH TIME ZONE, nationality VARCHAR(64), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_artist, load_date));",
    "CREATE TABLE s_buyer_details (hk_buyer CHAR(64) REFERENCES h_buyer(hk_buyer), load_date TIMESTAMP WITH TIME ZONE, buyer_vip_status VARCHAR(32), buyer_region_code VARCHAR(32), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_buyer, load_date));",
    "CREATE TABLE s_seller_details (hk_seller CHAR(64) REFERENCES h_seller(hk_seller), load_date TIMESTAMP WITH TIME ZONE, seller_risk_score VARCHAR(32), kyc_status VARCHAR(32), seller_contacts VARCHAR(128), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_seller, load_date));",
    "CREATE TABLE s_auction_house (hk_auction_house CHAR(64) REFERENCES h_auction_house(hk_auction_house), load_date TIMESTAMP WITH TIME ZONE, auction_house_name VARCHAR(64), branch_address VARCHAR(128), house_rating NUMERIC(2,1), house_contacts VARCHAR(64), commission_percent NUMERIC(4,2), api_endpoint_version VARCHAR(16), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_auction_house, load_date));",
    "CREATE TABLE s_category_details (hk_category CHAR(64) REFERENCES h_category(hk_category), load_date TIMESTAMP WITH TIME ZONE, classification VARCHAR(64), category_description VARCHAR(128), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_category, load_date));",
    "CREATE TABLE s_geography_details (hk_geography CHAR(64) REFERENCES h_geography(hk_geography), load_date TIMESTAMP WITH TIME ZONE, base_currency CHAR(3), regional_vat_rate NUMERIC(4,2), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_geography, load_date));",
    "CREATE TABLE s_lot_financials (hk_link_transaction CHAR(64) REFERENCES l_auction_transaction(hk_link_transaction), load_date TIMESTAMP WITH TIME ZONE, price_start NUMERIC(12,2), price_sale NUMERIC(12,2), hash_diff CHAR(64), record_source VARCHAR(64), PRIMARY KEY (hk_link_transaction, load_date));"
]

def apply_database_schema():
    """Автоматически накатывает всю DDL-структуру DWH в работающий Docker."""
    print("\n--- МОДУЛЬ ИНИЦИАЛИЗАЦИИ СХЕМЫ ХРАНИЛИЩА (DATA VAULT 2.0) ---")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        print(" 1. Активация криптографии...")
        cur.execute(INIT_CRYPTO_EXTENSION)
        
        print(" 2. Безопасное каскадное удаление старых таблиц (DROP)...")
        cur.execute(DROP_TABLES)
        
        print(" 3. Развертывание буферного слоя Staging...")
        cur.execute(CREATE_STAGING_TABLE)
        
        print(" 4. Развертывание ядра хранилища (Слой Хабов)...")
        for hub_query in CREATE_HUBS:
            cur.execute(hub_query)
            
        print(" 5. Развертывание слоя транзакционных связей (Слой Линков)...")
        for link_query in CREATE_LINKS:
            cur.execute(link_query)
            
        print(" 6. Развертывание исторического контекста (Слой Сателлитов)...")
        for sat_query in CREATE_SATELLITES:
            cur.execute(sat_query)
            
        conn.commit()
        cur.close()
        conn.close()
        print(" [✔] ИНИЦИАЛИЗАЦИЯ ЗАВЕРШЕНА. Вся структура Data Vault успешно развернута в СУБД.")
    except Exception as e:
        print(f" [!] КРИТИЧЕСКАЯ ОШИБКА создания таблиц: {e}")

if __name__ == "__main__":
    apply_database_schema()