import re
from dataclasses import dataclass
from icecream import ic

from config.const import TON_ORIGIN, PNWC_ORIGIN, POM_ORIGIN, EQUIPMENTS_ORIGIN, MONITORING_ORIGIN

# иерархия элементов каталога
# chain_items = ('Catalog', 'Chapter', 'Collection', 'Section', 'Subsection', 'Table',)
# --- > ПСМ Каталог PROJECT OUTLAY MODULE

# названия Справочников
teams = (
    'main', 'units', 'quotes', 'materials', 'machines', 'equipments',
    'pnwc_materials', 'pnwc_machines', 'pnwc_equipments',
)

# team, name, title, ID_parent, re_pattern, re_prefix
src_catalog_items = [
    (teams[0], 'main', 'справочник', None, r"^\s*0000\s*$", None),
    # список хранимых объектов
    (teams[1], 'Quote', 'Расценка', None, r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (teams[1], 'Material', 'Материал', None, r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (teams[1], 'Machine', 'Машина', None, r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),
    (teams[1], 'Equipment', 'Оборудование', None, r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", None),

    # разделы для каталога Расценок
    (teams[2], 'Chapter', 'глава', None, r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)\s"),
    (teams[2], 'Collection', 'сборник', 'Chapter', r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Сборник\s*((\d+)\.)\s"),
    (teams[2], 'Section', 'отдел', 'Collection', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*Отдел\s*((\d+)\.)\s"),
    (teams[2], 'Subsection', 'раздел', 'Section', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", r"^\s*Раздел\s*((\d+)\.)\s"),
    (teams[2], 'Table', 'таблица', 'Subsection', r"^\s*((\d+)\.(\d+)(-(\d+)){4})\s*$",
     r"^\s*Таблица\s*((\d+)\.(\d+)-(\d+)\.)\s"),

    # разделы для каталога Материалов глава 1
    (teams[3], 'chapter', 'глава', None, r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)\s"),
    (teams[3], 'section', 'раздел', 'chapter', r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Раздел\s*((\d+)\.)\s"),
    (teams[3], 'block_1', 'блок_1', 'section', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*((\d+)\.)\s"),
    (teams[3], 'block_2', 'блок_2', 'block_1', r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$", r"^\s*((\d+)\.(\d+)\.)\s"),

    # разделы для каталога Машин глава 2
    (teams[4], 'chapter', 'глава', None, r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)\s"),
    (teams[4], 'section', 'раздел', 'chapter', r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Раздел\s*((\d+)\.)\s"),
    (teams[4], 'block_1', 'блок_1', 'section', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*((\d+)\.)\s"),

    # разделы для каталога Оборудования глава 13
    (teams[5], 'chapter', 'глава', None, r"^\s*(\d+)\s*$", r"^\s*Глава\s*((\d+)\.)\s"),
    (teams[5], 'section', 'отдел', 'chapter', r"^\s*((\d+)\.(\d+))\s*$", r"^\s*Отдел\s*((\d+)\.)\s"),
    (teams[5], 'block_1', 'блок_1', 'section', r"^\s*((\d+)\.(\d+)-(\d+))\s*$", r"^\s*((\d+)\.)\s"),

    # разделы для каталога ПСМ Материалов. Глава 71
    (teams[6], 'Chapter', 'глава', None, r"^\s*(\d+)\s*$", None),
    (teams[6], 'Collection', 'сборник', 'Chapter', r"^\s*((\d+)\.(\d+))\s*$", None),

    # разделы для каталога ПСМ Машин. Глава 72
    (teams[7], 'Chapter', 'глава', None, r"^\s*(\d+)\s*$", None),
    (teams[7], 'Collection', 'сборник', 'Chapter', r"^\s*((\d+)\.(\d+))\s*$", None),

    # разделы для каталога ПСМ Оборудования. Глава 73
    (teams[8], 'Chapter', 'глава', None, r"^\s*(\d+)\s*$", None),
    (teams[8], 'Collection', 'сборник', 'Chapter', r"^\s*((\d+)\.(\d+))\s*$", None),

]

#
# Справочники для таблицы Периодов
# team, name, title, ID_parent, re_pattern, re_prefix
#
# ------------------ Справочники Периодов ----------------------------
#
# dir_period_holders = 'periods_holder'
dir_period_categories = 'periods_category'

period_directories = [
    (dir_period_categories, 'supplement', 'дополнение', None, None, None),
    (dir_period_categories, 'index', 'индекс', None, None, None),
    (dir_period_categories, 'special', 'специальный', None, None, None),

    # (dir_period_holders, 'ton', 'ТСН', None, None, None),
    # (dir_period_holders, 'equipments', 'оборудование', None, None, None),
    # (dir_period_holders, 'monitoring', 'мониторинг', None, None, None),
]

src_catalog_items += period_directories


@dataclass
class ItemCatalogDirectory:
    team: str | None
    name: str | None
    title: str | None
    parent: str | None
    re_pattern: str | None
    compiled: re.Pattern | None
    prefix: str | None


items_catalog: list[str: ItemCatalogDirectory] = [
    ItemCatalogDirectory(
        x[0].lower(), x[1].lower(), x[2].lower(),
        x[3].lower() if x[3] else None,
        x[4],
        re.compile(str(x[4])),
        x[5] if x[5] else None
    )
    for x in src_catalog_items
]

origin_items = (
            (TON_ORIGIN, 'Территориальные сметные нормативы'),
            (PNWC_ORIGIN, 'Нормативы Цен на Комплексы Работ'),
            (POM_ORIGIN, 'Проектно Сметные Модули'),
            (EQUIPMENTS_ORIGIN, "Оборудование"),
            (MONITORING_ORIGIN, "Мониторинг цен для расчета индексов")
        )

#
# ---------------------------------------------------------------------
#


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
    for origin in origin_items:
        ic(origin)
