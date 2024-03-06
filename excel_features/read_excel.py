import pandas
from files_features import output_message_exit


def read_excel_to_df(excel_file_name: str, sheet_name: str) -> pandas.DataFrame | None:
    """ Читает данные из XLS файла в DataFrame """
    try:
        df = pandas.read_excel(
            excel_file_name, sheet_name=sheet_name,  dtype="object", engine='openpyxl')
        if df.empty:
            output_message_exit(
                f"Данные из файла {excel_file_name!r}", f"Не прочитались.")
        return df
    except IOError as err:
        output_message_exit(f"Ошибка при чтении файла",
                            f" {excel_file_name!r}")


def read_csv_to_df(csv_file_name: str, delimiter: str = ";") -> pandas.DataFrame | None:
    """ Читает данные из CSV файла в df. """
    try:
        df = pandas.read_csv(
            csv_file_name, delimiter=delimiter, index_col=False, encoding="utf8", dtype=pandas.StringDtype())
        if df.empty:
            output_message_exit(
                f"Данные из файла {excel_file_name!r}", f"Не прочитались.")
        return df
    except IOError as err:
        output_message_exit(err, csv_file_name)
