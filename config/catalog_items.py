# import re
# from dataclasses import dataclass
from icecream import ic

src_catalog_items = [
        ('NULL', 'Directory', 'Справочник значений'),
        ('Directory', 'Chapter',	'Глава'),
        ('Chapter', 'Collection', 'Сборник'),
        ('Collection', 'Section', 'Отдел'),
        ('Section', 'Subsection', 'Раздел'),
        ('Subsection', 'Table', 'Таблица'),
        ('Table', 'Quote', 'Расценка'),
    ]

#
# _item_patterns: dict[str:str] = {
#     'directory': r"^\s*0000\s*$",
#     'chapter': r"^\s*(\d+)\s*$",
#     'collection': r"^\s*((\d+)\.(\d+))\s*$",
#     'section': r"^\s*((\d+)\.(\d+)-(\d+))\s*$",
#     'subsection': r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$",
#     'table': r"^\s*((\d+)\.(\d+)(-(\d+)){4})\s*$",
#     'quote': r"^\s*((\d+)\.(\d+)(-(\d+)){2})\s*$",
# }
#
# _compiled_item_patterns = {
#     'directory': re.compile(_item_patterns['directory']),
#     'chapter': re.compile(_item_patterns['chapter']),
#     'collection': re.compile(_item_patterns['collection']),
#     'section': re.compile(_item_patterns['section']),
#     'subsection': re.compile(_item_patterns['subsection']),
#     'table': re.compile(_item_patterns['table']),
#     'quote': re.compile(_item_patterns['quote']),
#
#     'subsection_groups': re.compile(r"(^\d+\.\d+-\d+-)(\d+)\s*"),
#     'wildcard': re.compile(r"[\t\n\r\f\v\s+]+"),
#     'code_valid_chars': re.compile(r"[^\d+.-]+"),
#     'digits': re.compile(r"[^\d+]+"),
#     'digits_dots': re.compile(r"[^\d+.]+"),
#
#     'table_prefix': re.compile(r"^\s*Таблица\s*((\d+)\.(\d+)-(\d+)\.)*"),  # Таблица 3.1-4.
#     'subsection_prefix': re.compile(r"^\s*Раздел\s*((\d+)\.)*"),  # Раздел 7.
#     'section_prefix': re.compile(r"^\s*Отдел\s*((\d+)\.)*"),  # Отдел 7.
#     'collection_prefix': re.compile(r"^\s*Сборник\s*((\d+)\.)*"),  # Сборник 7.
#     'chapter_prefix': re.compile(r"^\s*Глава\s*((\d+)\.)*"),  # Глава 7.
# }
#
#
# @dataclass
# class ItemCatalog:
#     rank: int
#     parent: int
#     name: str
#     pattern: str | None
#     compiled: re.Pattern | None
#     prefix: re.Pattern | None
#
#
# items_data: dict[str: ItemCatalog] = {
#     'directory': ItemCatalog(
#         100, 1, 'справочник', _item_patterns['directory'], _compiled_item_patterns['directory'], None
#     ),
#     'chapter': ItemCatalog(
#         90, 1, 'глава', _item_patterns['chapter'], _compiled_item_patterns['chapter'],
#         _compiled_item_patterns['chapter_prefix']
#     ),
#     'collection': ItemCatalog(
#         80, 2, 'сборник', _item_patterns['collection'], _compiled_item_patterns['collection'],
#         _compiled_item_patterns['collection_prefix']
#     ),
#     'section': ItemCatalog(
#         70, 3, 'отдел', _item_patterns['section'], _compiled_item_patterns['section'],
#         _compiled_item_patterns['section_prefix']
#     ),
#     'subsection': ItemCatalog(
#         60, 4, 'раздел', _item_patterns['subsection'], _compiled_item_patterns['subsection'],
#         _compiled_item_patterns['subsection_prefix']
#     ),
#     'table': ItemCatalog(
#         50, 5, 'таблица', _item_patterns['table'], _compiled_item_patterns['table'],
#         _compiled_item_patterns['table_prefix']
#     ),
#     'quote': ItemCatalog(
#         40, 6, 'расценка', _item_patterns['quote'], _compiled_item_patterns['quote'], None
#     ),
# }

if __name__ == "__main__":
    ic(src_catalog_items)
