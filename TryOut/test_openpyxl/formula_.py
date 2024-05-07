import openpyxl
wb = openpyxl.load_workbook("test.xlsx")

ws = wb.active
ws = wb['Test']

for i in range(10):
    v = ws.cell(row=1, column=i+1).value
    print(v)

ws["A5"] = "=SUM(5, 4)"
ws["B5"] = "=IF(B1, 55, 33)"
ws.cell(row=4, column=3).value = "=IF(B1, 55, 33)"

wb.save("test.xlsx")
wb.close()
