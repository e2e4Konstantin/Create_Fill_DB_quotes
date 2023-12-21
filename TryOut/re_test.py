import re
des = 'Раздел 0. стоимость транспортных затрат по общим положениям'


prefix = re.compile(r"^\s*Раздел\s*((\d+)\.)*")

x = prefix.sub('***', des)
print(x)
