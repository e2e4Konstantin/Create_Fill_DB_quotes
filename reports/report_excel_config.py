
from openpyxl.styles import Font, PatternFill, numbers, DEFAULT_FONT, Color
from config import ExcelBase


class ExcelReport(ExcelBase):
    def __init__(self, filename: str = None):
        super().__init__(filename)
        self.sheet_names = [
            "transport",
            "monitoring",
        ]
        self.tab_colors = {
            "transport": "00FF9900",
            "monitoring": "0099CC00",
            "header": "00E4DFEC",
        }
        self.default_font = Font(name="Arial", size=8, bold=False, italic=False)
        self.bold_font = Font(name="Arial", size=8, bold=True, italic=False)
        self.green_font = Font(name="Arial", size=8, bold=True, color="006600")
        self.grey_font = Font(name="Arial", size=8, bold=False, color=Color("808080"))
        self.blue_font = Font(name="Arial", size=8, bold=False, color="1F497D")

    #
    def __enter__(self):
        super().__enter__()
        return self

    def create_sheets(self):
        """удаляет все листы в книге и создает новые"""
        if self.workbook:
            for sheet in self.workbook.worksheets:
                self.workbook.remove(sheet)
            # DEFAULT_FONT = self.default_font
            for i, name in enumerate(self.sheet_names):
                self.sheet = self.workbook.create_sheet(name)
                self.sheet.font = self.default_font
                self.sheet.sheet_properties.tabColor = self.tab_colors[name]


    def write_header(self, sheet_name: str, header: list, row: int = 1):
        """записывает заголовок в лист"""
        # self.create_sheets()
        self.worksheet = self.workbook[sheet_name]
        # self.worksheet.append(header)

        for col in range(1, len(header) + 1):
            self.worksheet.cell(row=row, column=col).value = header[col - 1]
            self.worksheet.cell(row=row, column=col).font = self.default_font
            self.worksheet.cell(row=row, column=col).fill = PatternFill(
                "solid", fgColor=self.tab_colors["header"]
            )

    def write_row(self, sheet_name: str, values: list, row_index: int = 2):
        """записывает строку в лист"""
        worksheet = self.workbook[sheet_name]
        for column_index, value in enumerate(values, start=1):
            worksheet.cell(row=row_index, column=column_index).value = value
            # worksheet.cell(row=row_index, column=column_index).font = self.font

    def write_format(self, sheet_name: str, row_index: int, len_row):
        """записывает строку в лист"""

        self.worksheet = self.workbook[sheet_name]
        for column_index in range(14, 18):
            cell = self.worksheet.cell(row=row_index, column=column_index)
            cell.font = self.default_font
            cell.number_format = numbers.FORMAT_NUMBER_00


        self.worksheet.cell(row=row_index, column=1).font = self.bold_font

        price_cells = [2, 10, 12]
        for column_index in price_cells:
            cell = self.worksheet.cell(row=row_index, column=column_index)
            cell.font = self.green_font
            cell.number_format = "# ##0.00"

        for col in range(3, 9):
            cell = self.worksheet.cell(row=row_index, column=col)
            cell.font = self.grey_font

        index_cells = (9, 11)
        for column_index in index_cells:
            cell = self.worksheet.cell(row=row_index, column=column_index)
            cell.font = self.blue_font

