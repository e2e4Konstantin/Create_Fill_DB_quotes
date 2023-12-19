import re
from dataclasses import dataclass
from icecream import ic

# иерархия элементов каталога
# chain_items = ('Catalog', 'Chapter', 'Collection', 'Section', 'Subsection', 'Table',)

# названия справочников
teams = ['main', 'units', 'quotes', 'materials', 'machines', 'equipments']

src_catalog_items = [
    (teams[0], 'main', 'справочник', None, r"^\s*0000\s*$", None),
    # список хранимых объектов
    (teams[1], 'Quote',      'Расценка',     None, r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (teams[1], 'Material',   'Материал',     None, r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (teams[1], 'Machine',    'Машина',       None, r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (teams[1], 'Equipment',  'Оборудование', None, r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),

    # разделы для каталога Расценок
    (teams[2], 'Chapter',    'Глава',    None,      r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)*"),
    (teams[2], 'Collection', 'Сборник',  'Chapter',      r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Сборник\s*((\d+)\.)*"),
    (teams[2], 'Section',    'Отдел',    'Collection',   r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*Отдел\s*((\d+)\.)*"),
    (teams[2], 'Subsection', 'Раздел',   'Section',      r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", r"^\s*Раздел\s*((\d+)\.)*"),
    (teams[2], 'Table',      'Таблица',  'Subsection',   r"^\s*((\d+)\.(\d+)(-(\d+)){4})\s*$", r"^\s*Таблица\s*((\d+)\.(\d+)-(\d+)\.)*"),

    # разделы для каталога Материалов глава 1
    (teams[3], 'chapter', 'глава', None, r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)*"),
    (teams[3], 'section', 'раздел', 'chapter', r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Раздел\s*((\d+)\.)*"),
    (teams[3], 'block_1', 'блок_1', 'section', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*((\d+)\.)*"),
    (teams[3], 'block_2', 'блок_2', 'block_1', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", r"^\s*((\d+)\.(\d+)\.)*"),

    # разделы для каталога Машин глава 2
    (teams[4], 'chapter', 'глава', None, r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)*"),
    (teams[4], 'section', 'раздел', 'chapter', r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Раздел\s*((\d+)\.)*"),
    (teams[4], 'block_1', 'блок_1', 'section', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*((\d+)\.)*"),
    (teams[4], 'block_2', 'блок_2', 'block_1', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", r"^\s*((\d+)\.(\d+)\.)*"),

    # разделы для каталога Оборудования глава 13
    (teams[5], 'chapter', 'глава', None, r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)*"),
    (teams[5], 'section', 'отдел', 'chapter', r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Отдел\s*((\d+)\.)*"),
    (teams[5], 'block_1', 'блок_1', 'section', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*((\d+)\.)*"),
]


@dataclass
class ItemCatalogDirectory:
    team:       str | None
    name:       str | None
    title:      str | None
    parent:     str | None
    re_pattern: str | None
    compiled:   re.Pattern | None
    prefix:     re.Pattern | None


items_catalog: list[str: ItemCatalogDirectory] = [
    ItemCatalogDirectory(
        x[0].lower(), x[1].lower(), x[2].lower(), x[3].lower() if x[3] else None, x[4],
        re.compile(str(x[4])), re.compile(str(x[5]))
    )
    for x in src_catalog_items
]


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
    # ic(team)
    # ic(src_catalog_items)

    for data in items_catalog:
        ic(data)
