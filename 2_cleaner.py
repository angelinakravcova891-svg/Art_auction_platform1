import os
import hashlib
import pandas as pd
from datetime import datetime, timezone

def clean_and_prepare_big_data():
    # Адаптировано под имя выходного файла из нашего Модуля 1 (1_fetcher.py)
    input_file = 'data_lake/ArtMarket_Synthetic_BigData.csv'
    output_folder = 'data_lake_clean' 
    output_file = os.path.join(output_folder, 'real_live_prepared_data.csv')
    
    # Создаем папку назначения для чистого слоя
    if not os.path.exists(output_folder): 
        os.makedirs(output_folder)

    # Проверка физического наличия исходного файла Озера данных
    if not os.path.exists(input_file):
        print(f" [!] ОШИБКА: Сырой файл '{input_file}' не найден.")
        print(" -> Сначала запустите Модуль сбора данных командой: python3 1_fetcher.py и выберите режим 2.")
        return

    # Карта маппинга колонок из сырого слоя парсера в технический контракт СУБД
    translate_columns = {
        'lot_id_natural': 'lot_id_modified',
        'reg_num_lota': 'reg_num_lota_mod',
        'god_provedeniya': 'god_provedeniya',
        'kategoriya_arta': 'kategoriya_arta',
        'naimenovanie_lota': 'naimenovanie_lota_mod',
        'period': 'period',
        'artist_id': 'artist_id_raw',
        'author_name_natural': 'author_name_natural',
        'nationality': 'nationality',
        'material_tech': 'material_tech',
        'gabariti': 'gabariti',
        'auction_house_id': 'auction_house_id',
        'auction_house': 'auction_house',
        'classification': 'classification',
        'price_start': 'price_start',
        'price_sale': 'price_sale',
        'ID_Покупателя_естественный': 'buyer_id_token',  # Точное совпадение с CSV_HEADERS из 1_fetcher.py
        'ID_Продавца_естественный': 'seller_id_token',   # Точное совпадение с CSV_HEADERS из 1_fetcher.py
        'city_session': 'city_session',
        'record_source': 'record_source'
    }

    # Порядок сырых бизнес-колонок для выгрузки (БЕЗ ХЭШЕЙ!)
    export_cols = [
        'lot_id_modified', 'reg_num_lota_mod', 'god_provedeniya', 'kategoriya_arta',
        'naimenovanie_lota_mod', 'period', 'artist_id_raw', 'author_name_natural',
        'nationality', 'material_tech', 'gabariti', 'auction_house_id',
        'auction_house', 'classification', 'city_session', 'price_start', 'price_sale',
        'buyer_id_token', 'seller_id_token', 'load_date', 'record_source'
    ]

    print(f"\n--- МОДУЛЬ 2: СЛОЙ ОЧИСТКИ И ПОДГОТОВКИ СЫРЫХ ДАННЫХ BIG DATA ---")
    print(f" Исходный файл: {input_file}")
    print(" Запуск потокового чтения для сохранения сырой текстовой структуры...")
    
    # Удаляем старый файл очищенных данных, если он существовал от прошлых запусков
    if os.path.exists(output_file):
        os.remove(output_file)

    # Метка времени загрузки батча для ИБ-аудита платформы
    load_date = datetime.now(timezone.utc).isoformat()
    chunk_size = 100000  # Читаем порциями по 100 тысяч строк для стабильности памяти Mac
    processed_rows = 0
    is_first_chunk = True

    try:
        # Потоковый Big Data итератор по файлу 2 ГБ с явным указанием кодировки UTF-8
        for chunk in pd.read_csv(input_file, usecols=translate_columns.keys(), chunksize=chunk_size, low_memory=False, encoding='utf-8'):
            
            # 1. Быстрое переименование колонок по карте маппинга
            chunk = chunk.rename(columns=translate_columns)
            
            # 2. Проставляем системную дату загрузки
            chunk['load_date'] = load_date
            
            # Сортируем колонки согласно финальному списку экспорта
            chunk_export = chunk[export_cols]
            
            # 3. Инкрементальная дозапись в итоговый чистый CSV
            chunk_export.to_csv(output_file, mode='a', index=False, header=is_first_chunk, encoding='utf-8')
            
            is_first_chunk = False
            processed_rows += len(chunk)
            print(f"   -> Успешно обработано и переведено {processed_rows} сырых строк...")
            
        print("\n Сборка сырого пакета завершена. Расчет глобальной подписи подлинности файла...")
        
        # 4. Контур безопасности: расчет хэш-суммы всего результирующего файла для защиты от подмены на диске
        hasher = hashlib.sha256()
        with open(output_file, "rb") as f_out:
            for chunk_bytes in iter(lambda: f_out.read(65536), b""): 
                hasher.update(chunk_bytes)
        
        # Запись контрольной суммы в манифест безопасности
        sha256_path = output_file + ".sha256"
        with open(sha256_path, "w") as hash_file:
            hash_file.write(hasher.hexdigest())
            
        print(f" [УСПЕХ] Сырой пакет полностью сформирован и подписан: {output_file}")
        print(f" [УСПЕХ] Файл цифровой подписи SHA-256 сохранен: {sha256_path}")
        print(f" Итоговый размер чистого текстового файла: {os.path.getsize(output_file) / (1024*1024):.2f} МБ")
        
    except Exception as e:
        print(f" !!! Критический сбой слоя очистки данных: {e}")

if __name__ == "__main__":
    clean_and_prepare_big_data()