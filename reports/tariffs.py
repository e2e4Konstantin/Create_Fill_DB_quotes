import sqlite3
from icecream import ic
from config import ExcelBase
from config import dbTolls, LocalData, ROUNDING


def _tariff_file_parching(tariffs_file_name: str, sheet_name: str = "Тарифы"):
    tariff_codes = ('1.1-1-118', '1.1-1-41', '1.1-1-1268', '1.1-1-2238', '1.1-1-1920')
    # ic(tariffs_file_name)
    with ExcelBase(tariffs_file_name) as file:
        sheet = file.workbook[sheet_name]
        max_rows = sheet.max_row
        max_columns = sheet.max_column
        ic(max_rows, max_columns)
        for row in range(1, max_rows + 1):
            for col in range(1, max_columns + 1):
                value = sheet.cell(row=row, column=col).value
                value = str(value).strip() if value is not None else None
                if value and value in tariff_codes:
                    index = tariff_codes.index(value)
                    ic(row, col, value, index)

    return []


def tariffs_monitoring_report(
    db_name: str,
    tariff_file_name: str,
    sheet_name: str = "tariffs_prices",
    file_name: str = "report_monitoring.xlsx",
):
    ic()
    tariffs = _tariff_file_parching(tariff_file_name)
    ic(len(tariffs))
    ic(tariffs[0])



if __name__ == "__main__":
    location = "office"  # office  # home
    local = LocalData(location)
    ic()
    report_file_name = "report_monitoring.xlsx"
    tariffs_file_name = r"C:\Users\kazak.ke\Documents\Задачи\5_Надя\исходные_данные\тарифы\Тарифы на 20.05.2024.xlsx"

    tariffs = _tariff_file_parching(tariffs_file_name=tariffs_file_name)
    ic(len(tariffs))
    ic(tariffs)

    # tariffs_monitoring__report(db_name=local.db_file,
        # tariffs_file_name=tariffs_file_name,
        # sheet_name="tariffs_prices",
        # file_name=report_file_name,)
