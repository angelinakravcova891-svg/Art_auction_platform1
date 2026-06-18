import os
import csv

def simulate_cyber_attack():
    print("\n--- МОДУЛЬ 5: СИМУЛЯЦИЯ КИБЕРАТАКИ (ЗАПИСЬ НЕОБНАРУЖИВАЕМЫХ ИЗМЕНЕНИЙ) ---")
    
    # Точный путь к очищенному файлу из Модуля 2 (2_cleaner.py)
    target_file = 'data_lake_clean/real_live_prepared_data.csv'
    
    if not os.path.exists(target_file):
        print(f"ОШИБКА: Файл {target_file} не найден.")
        print(" -> Сначала выполните Шаг 2 командой: python3 2_cleaner.py")
        return

    print(f"Взлом файловой системы... Внедрение модификации в файл: {target_file}")
    
    # Безопасное чтение CSV-структуры с учетом экранирования кавычек
    lines = []
    try:
        with open(target_file, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            headers = next(reader)
            lines.append(headers)
            for row in reader:
                lines.append(row)
    except Exception as e:
        print(f"ОШИБКА при чтении файла: {e}")
        return
    
    if len(lines) > 1:
        headers = lines[0]
        original_row = list(lines[1])  # Сохраняем копию первой строки данных для лога
        
        # Динамически находим индекс колонки с ценой продажи
        try:
            price_sale_idx = headers.index('price_sale')
        except ValueError:
            print("ОШИБКА: Колонка 'price_sale' не найдена в заголовках файла.")
            return
            
        # Пририсовываем фальшивый ноль к цене (увеличиваем цену продажи лота в 10 раз!)
        old_price = lines[1][price_sale_idx]
        
        # Защита от модификации пустых или нечисловых значений
        if old_price and old_price != "0":
            lines[1][price_sale_idx] = old_price + "0"
        else:
            lines[1][price_sale_idx] = "500000"  # Если лот был не продан, рисуем $500k
            
        new_price = lines[1][price_sale_idx]
        
        # Перезаписываем файл обратно с сохранением CSV-формата
        try:
            with open(target_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(lines)
        except Exception as e:
            print(f"ОШИБКА при перезаписи файла: {e}")
            return
            
        print("\n[!] АТАКА ЗАВЕРШЕНА. В финансовые данные скрытно внедрены изменения.")
        print(f" Исходная строка:  {original_row}")
        print(f" Измененная строка: {lines[1]}")
        print(f" Результат взлома: Цена продажи изменена с {old_price} USD на {new_price} USD")
        print(" Уведомление: Контрольная сумма .sha256 намеренно ОСТАВЛЕНА СТАРЕЙ.")
    else:
        print("Файл пуст, атака не удалась.")

if __name__ == "__main__":
    simulate_cyber_attack()