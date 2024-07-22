import pandas as pd
import matplotlib.pyplot as plt

# Путь к файлу CSV
file_path = 'full.csv'

# Загружаем данные из CSV файла
df = pd.read_csv(file_path)

# Сначала удалим строки, где значение в столбце J равно нулю
initial_row_count = len(df)
df = df[df.iloc[:, 9] != 0]
rows_removed = initial_row_count - len(df)

# Добавляем новый столбец с типом плана
def get_plan_type(value):
    if 1 <= value <= 3:
        return 'free plan'
    elif 4 <= value <= 10:
        return 'monthly'
    elif 10 < value <= 30:
        return '3months'
    elif 50 <= value <= 80:
        return 'annual'
    elif 80 < value <= 200:
        return 'pass'
    else:
        return 'unknown'

df['Plan Type'] = df.iloc[:, 9].apply(get_plan_type)

# Подсчитываем количество каждой категории планов
plan_counts = df['Plan Type'].value_counts()

# Общее количество транзакций
total_transactions = len(df)

# Количество уникальных кошельков (столбец E)
unique_wallets = df.iloc[:, 4].nunique()

# Количество кошельков, с которых было совершено больше одной транзакции
wallet_transaction_counts = df.iloc[:, 4].value_counts()
wallets_more_than_one_transaction = wallet_transaction_counts[wallet_transaction_counts > 1].count()

# Вывод результатов
print("Количество каждой категории планов:")
for plan, count in plan_counts.items():
    print(f"{plan}: {count}")
print(f"\nОбщее количество транзакций: {total_transactions}")
print(f"Количество удаленных строк: {rows_removed}")
print(f"Количество уникальных кошельков: {unique_wallets}")
print(f"Количество кошельков, с которых было совершено больше одной транзакции: {wallets_more_than_one_transaction}")


# Подсчет количества кошельков, совершивших от одной до десяти транзакций
wallets_transaction_counts_range = wallet_transaction_counts.value_counts().sort_index()
for i in range(1, 11):
    count = wallets_transaction_counts_range.get(i, 0)
    print(f"Количество кошельков, совершивших {i} транзакцию(й): {count}")
