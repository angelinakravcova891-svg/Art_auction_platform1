import os
import hashlib
import io
import pandas as pd

def run_analysis():
    print("\n--- МОДУЛЬ 4: СКАНИРОВАНИЕ БЕЗОПАСНОСТИ И ГЛУБОКАЯ АРТ-АНАЛИТИКА ---")
    folder = 'data_lake_clean' 
    
    if not os.path.exists(folder):
        print(f"ОШИБКА: Директория '{folder}' не найдена. Сначала выполните Шаг 2 (2_cleaner.py).")
        return

    files = [f for f in os.listdir(folder) if f.endswith('.csv')]
    if not files:
        print(f"ПРЕДУПРЕЖДЕНИЕ: В папке '{folder}' отсутствуют файлы данных.")
        return

    corrupted_files = 0
    
    # Инициализация глобальных переменных для потоковой агрегации Big Data
    total_lots = 0
    total_turnover_usd = 0.0
    total_start_usd = 0.0
    
    # Словари для накопления группировок без сохранения строк в памяти
    geo_stats_dict = {}
    cat_stats_dict = {}
    
    print("Запуск сквозной верификации криптографической целостности Озера...")
    for file in files:
        path = os.path.join(folder, file)
        hash_path = path + ".sha256"
        
        if not os.path.exists(hash_path):
            print(f" 🔥 Файл {file}: [КРИТИЧЕСКАЯ УГРОЗА] Отсутствует цифровая подпись .sha256!")
            corrupted_files += 1
            continue
            
        # 1. Потоковое вычисление хэша (Защита памяти RAM от файлов > 2 ГБ)
        try:
            hasher = hashlib.sha256()
            with open(path, "rb") as f_hash:
                for chunk_bytes in iter(lambda: f_hash.read(65536), b""):
                    hasher.update(chunk_bytes)
            current_hash = hasher.hexdigest()
        except Exception as e:
            print(f" 🔥 Ошибка доступа к файлу {file}: {e}")
            corrupted_files += 1
            continue
        
        # Читаем эталонный хэш
        with open(hash_path, "r", encoding="utf-8") as hash_file:
            expected_hash = hash_file.read().strip()
            
        if current_hash != expected_hash:
            print(f" 🔥 Файл {file}: [КОНТРОЛЬ ЦЕЛОСТНОСТИ НАРУШЕН] Обнаружен факт модификации данных на диске!")
            corrupted_files += 1
            continue
            
        print(f" ✅ Файл {file}: [ЦЕЛОСТНОСТЬ ПОДТВЕРЖДЕНА]")
        
        # 2. Потоковый расчет метрик прямо во время чтения (Stream Aggregation)
        try:
            useful_cols = ['lot_id_modified', 'naimenovanie_lota_mod', 'kategoriya_arta', 'price_start', 'price_sale', 'city_session']
            chunk_iterator = pd.read_csv(
                path, 
                usecols=useful_cols,
                chunksize=100000,
                low_memory=False,
                encoding='utf-8'
            )
            
            for chunk_df in chunk_iterator:
                # Жестко переводим цены в числовой формат для математических расчетов
                chunk_df['price_start'] = pd.to_numeric(chunk_df['price_start'], errors='coerce').fillna(0)
                chunk_df['price_sale'] = pd.to_numeric(chunk_df['price_sale'], errors='coerce').fillna(0)
                
                # Инкрементально наращиваем базовые метрики
                total_lots += len(chunk_df)
                total_turnover_usd += chunk_df['price_sale'].sum()
                total_start_usd += chunk_df['price_start'].sum()
                
                # Инкрементальная группировка по городам
                for city, val in chunk_df.groupby('city_session')['price_sale'].sum().items():
                    geo_stats_dict[city] = geo_stats_dict.get(city, 0.0) + val
                    
                # Инкрементальная группировка по категориям
                for cat, val in chunk_df.groupby('kategoriya_arta')['price_sale'].sum().items():
                    cat_stats_dict[cat] = cat_stats_dict.get(cat, 0.0) + val
                    
        except Exception as e:
            print(f"Ошибка парсинга структуры в {file}: {e}")
            corrupted_files += 1

    # --- ФИНАЛЬНЫЙ ВЫВОД ОТЧЕТА ---
    print(f"\n" + "="*65)
    print(f"     ОТЧЕТ КОНТУРА БЕЗОПАСНОСТИ И АГРЕГАЦИИ АРТ-АУКЦИОНОВ   ")
    print(f"="*65)
    print(f" Статус защищенности платформы: {'[БЕЗОПАСЕН]' if corrupted_files == 0 else '[ОБНАРУЖЕНА АТАКA / ДАННЫЕ СКОМПРОМЕТИРОВАНЫ]'}")
    print(f" Количество компрометированных файлов: {corrupted_files}")
    print(f"-----------------------------------------------------------------")
    
    if corrupted_files == 0 and total_lots > 0:
        total_margin = total_turnover_usd - total_start_usd
        
        # 1. Базовые метрики
        print(f" Всего успешно агрегировано лотов: {total_lots:,.0f}".replace(",", " "))
        print(f" Суммарный финансовый оборот торгов: ${total_turnover_usd:,.2f}".replace(",", " "))
        print(f" Суммарный прирост в ходе торгов:    ${total_margin:,.2f}".replace(",", " "))
        print(f" Средняя стоимость произведения:     ${total_turnover_usd / total_lots:,.2f}".replace(",", " "))
        print(f"-----------------------------------------------------------------")
        
        # 2. Бизнес-аналитика по Городам
        print(" 📍 ТОП АУКЦИОННЫХ СЕССИЙ ПО ВЫРУЧКЕ:")
        sorted_geo = sorted(geo_stats_dict.items(), key=lambda x: x[1], reverse=True)
        for city, val in sorted_geo:
            print(f"  • {city:<12} | Выручка: ${val:,.2f}".replace(",", " "))
        print(f"-----------------------------------------------------------------")
            
        # 3. Бизнес-аналитика по Категориям
        print(" 🎨 РАСПРЕДЕЛЕНИЕ ВЫРУЧКИ ПО КАТЕГОРИЯМ АРТА:")
        sorted_cat = sorted(cat_stats_dict.items(), key=lambda x: x[1], reverse=True)
        for cat, val in sorted_cat:
            print(f"  • {cat:<30} | Выручка: ${val:,.2f}".replace(",", " "))
            
    else:
        print(" Аналитический расчет заблокирован автоматикой ИБ из-за нарушения целостности.")
    print(f"="*65)

if __name__ == "__main__":
    run_analysis()