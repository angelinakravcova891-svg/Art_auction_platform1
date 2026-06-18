import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import os

file_name = 'ml_features_vkr.csv'

if not os.path.exists(file_name):
    raise FileNotFoundError(f"ОШИБКА: Файл '{file_name}' не найден в корне проекта!")

print("Шаг 1: Потоковое чтение реальных Big Data признаков из СУБД...")
df_real = pd.read_csv(file_name)
print(f" [✔] УСПЕХ! В модель загружено: {len(df_real):,} настоящих строк из вашего Data Vault 2.0")

print("\n Шаг 2: Feature Engineering (Генерация признаков Feature Store на ходу)...")
df_real['price_start'] = pd.to_numeric(df_real['price_start'], errors='coerce').fillna(1.0)
df_real['price_sale'] = pd.to_numeric(df_real['price_sale'], errors='coerce').fillna(1.0)
df_real['price_ratio'] = df_real['price_sale'] / (df_real['price_start'] + 1)
df_real['price_delta'] = df_real['price_sale'] - df_real['price_start']

X = df_real[['price_start', 'price_sale', 'price_ratio', 'price_delta']]
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print("\n Шаг 3: Обучение модели Isolation Forest на вашем Mac...")
model = IsolationForest(n_estimators=100, contamination=0.001, random_state=42, n_jobs=-1)
model.fit(X_scaled)
df_real['seller_risk_score'] = np.where(model.predict(X_scaled) == -1, 1, 0)

print("\n Шаг 4: Формирование отчета аудита качества данных (Data Quality Report)...")
total_anomalies = df_real['seller_risk_score'].sum()
print(f" [✔] Всего в вашей СУБД выявлено и изолировано подозрительных транзакций: {total_anomalies:,}")

print("\n ТОП-5 АНОМАЛЬНЫХ ТРАНЗАКЦИЙ С РЕАЛЬНЫМИ ХЭШ-КЛЮЧАМИ ИЗ ВАШЕЙ БАЗЫ:")
print(df_real[df_real['seller_risk_score'] == 1][['hk_link_transaction', 'price_start', 'price_sale', 'price_ratio']].head(5))

print("\n🎨 Шаг 5: Отрисовка графического отчета...")
plt.figure(figsize=(12, 6), dpi=100)
sns.set_style("whitegrid")
df_plot = df_real.sample(min(30000, len(df_real)), random_state=42)

plt.scatter(df_plot[df_plot['seller_risk_score'] == 0]['price_start'], df_plot[df_plot['seller_risk_score'] == 0]['price_sale'], c='teal', alpha=0.4, label='Валидированный поток данных хранилища', s=12)
plt.scatter(df_plot[df_plot['seller_risk_score'] == 1]['price_start'], df_plot[df_plot['seller_risk_score'] == 1]['price_sale'], c='crimson', alpha=0.9, label='Изолировано контуром ML-Антифрода', s=35, edgecolors='black')
plt.title('MLOps-контур выявления ИБ-аномалий в СУБД PostgreSQL (Isolation Forest)', fontsize=13, fontweight='bold', pad=15)
plt.xlabel('Стартовая цена лота ($)', fontsize=11)
plt.ylabel('Финальная цена продажи лота ($)', fontsize=11)
plt.xscale('log')
plt.yscale('log')
plt.legend(fontsize=10, loc='upper left')
plt.savefig('real_ml_anomaly_vkr.png', bbox_inches='tight')
print("\n [УСПЕХ] Истинный график 'real_ml_anomaly_vkr.png' сохранен в корень проекта!")
plt.show()