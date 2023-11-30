import re
from dataclasses import dataclass
from icecream import ic

# Иерархические типы объектов каталога.
# Иерархия задается родительским объектом для текущего.
# Корневой элемент не имеет родителя.
src_catalog_items = (
    (None, 'Directory', 'Справочник', None, None),
    ('Directory', 'Catalog', 'Каталог', r"^\s*0000\s*$", None),
    ('Catalog', 'Chapter', 'Глава', r"^\s*(\d+)\s*$", re.compile(r"^\s*Глава\s*((\d+)\.)*")),
    ('Chapter', 'Collection', 'Сборник', r"^\s*((\d+)\.(\d+))\s*$", re.compile(r"^\s*Сборник\s*((\d+)\.)*")),
    ('Collection', 'Section', 'Отдел', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", re.compile(r"^\s*Отдел\s*((\d+)\.)*")),
    ('Section', 'Subsection', 'Раздел', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", re.compile(r"^\s*Раздел\s*((\d+)\.)*")),
    ('Subsection', 'Table', 'Таблица', r"^\s*((\d+)\.(\d+)(-(\d+)){4})\s*$",
     re.compile(r"^\s*Таблица\s*((\d+)\.(\d+)-(\d+)\.)*")),
    ('Table', 'Quote', 'Расценка', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None)
)


@dataclass
class ItemCatalogDirectory:
    name: str | None
    parent: str | None
    pattern: str | None
    compiled: re.Pattern | None
    prefix: re.Pattern | None


items_catalog: dict[str: ItemCatalogDirectory] = {x[1]: ItemCatalogDirectory(x[2], x[0], x[3], None, x[4])
                                                  for x in src_catalog_items}

for item in items_catalog:
    items_catalog[item].compiled = re.compile(str(items_catalog[item].pattern))

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
    ic(src_catalog_items)
    ic(items_catalog)
