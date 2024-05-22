import openpyxl
import csv

source_file = r"C:\Users\kazak.ke\Documents\Задачи\5_Надя\исходные_данные\транспорт\Раздел_0_Индексы_под загрузку_21.05.2024.xlsx"
sheet_name = "Лист1"

output_file = r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Мониторинг\transport_monitoring_result_72_212.csv"


wb = openpyxl.load_workbook(source_file)
sheet = wb[sheet_name]
print(sheet)


columns_to_write = ["Код", "current_price"]

Код	Наименование	Ед.изм.	в базисном уровне цен на 01.01.2000	Индекс текущий	в текущем уровне цен


with open(output_file, "w", newline="") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(columns_to_write)

    for row in sheet.iter_rows(min_row=1, values_only=True):
        # Only write the rows where all the columns are present
        if None not in [row[int(col) - 1] for col in columns_to_write]:
            # Write the selected columns to the CSV file
            writer.writerow([row[int(col) - 1] for col in columns_to_write])