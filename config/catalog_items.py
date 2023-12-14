import re
from dataclasses import dataclass
from icecream import ic


team = ['clip', 'unit']

src_catalog_items = (
    (team[0], 'Catalog', 'Каталог', r"^\s*0000\s*$", None),
    (team[0], 'Chapter', 'Глава', r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)*"),
    (team[0], 'Collection', 'Сборник', r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Сборник\s*((\d+)\.)*"),
    (team[0], 'Section', 'Отдел', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*Отдел\s*((\d+)\.)*"),
    (team[0], 'Subsection', 'Раздел', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", r"^\s*Раздел\s*((\d+)\.)*"),
    (team[0], 'Table', 'Таблица', r"^\s*((\d+)\.(\d+)(-(\d+)){4})\s*$", r"^\s*Таблица\s*((\d+)\.(\d+)-(\d+)\.)*"),

    (team[1], 'Quote', 'Расценка', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (team[1], 'Material', 'Материал', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (team[1], 'Machine', 'Машина', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (team[1], 'Equipment', 'Оборудование', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
)


@dataclass
class ItemCatalogDirectory:
    team: str | None
    name: str | None
    compiled: re.Pattern | None
    prefix: re.Pattern | None


items_catalog: dict[str: ItemCatalogDirectory] = {
    x[1]: ItemCatalogDirectory(x[0], x[2], re.compile(str(x[3])), re.compile(str(x[4])))
    for x in src_catalog_items
}


Physical_Property = (
    ('Directory', 'Measures', 'Измерения'),
    ('Measures', 'Square', 'площадь',),
    ('Square', 'MeterSquare', 'метр квадратный'),
    ('Measures', 'Mass', 'масса'),
    ('Measures', 'Quantity', 'Количество'),
    ('Quantity', 'Unit', 'штука'),
    ('Measures', 'Time', 'Время'),
    ('Measures', 'Length', 'Длинна'),
    ('Measures', 'Volume', 'Объем'),
    ('Volume', 'CubicMeter', 'метр кубический'),
)

if __name__ == "__main__":
    # ic(src_catalog_items)
    # ic(items_catalog)
    for item, data in items_catalog.items():
        ic(item, data.name, data.team)
