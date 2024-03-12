import re
from fastnumbers import isfloat, isint
from dateutil import parser

# from config import items_catalog

_re_compiled_patterns = {
    'subsection_groups': re.compile(r"(^\d+\.\d+-\d+-)(\d+)\s*"),
    'wildcard': re.compile(r"[\t\n\r\f\v\s+]+"),
    'code_valid_chars': re.compile(r"[^\d+.-]+"),
    'digits': re.compile(r"[^\d+]+"),
    'digits_dots': re.compile(r"[^\d+.]+"),
}


def remove_wildcard(source: str = None) -> str | None:
    """ Удаляет из строки переносы строк и табуляции, одиночные пробелы оставляет """
    return re.sub(_re_compiled_patterns['wildcard'], r" ", source.strip()) if source else None


def clear_code(source: str = None) -> str | None:
    """ Удаляет из строки все символы кроме (чисел, '.', '-') """
    return re.sub(_re_compiled_patterns['code_valid_chars'], r"", source)


def keep_just_numbers(source: str = None) -> str | None:
    """ Удаляет из строки все символы кроме чисел """
    if source:
        return re.sub(_re_compiled_patterns['digits'], r"", source)
    return ""


def keep_just_numbers_dots(source: str = None) -> str | None:
    """ Удаляет из строки все символы кроме чисел и точек"""
    if source:
        return re.sub(_re_compiled_patterns['digits_dots'], r"", source)
    return ""


def title_catalog_extraction(title: str, pattern_prefix: str) -> str | None:
    """ Удаляет лишние пробелы. Удаляет из заголовка префикс. В первом слове делает первую букву заглавной. """
    title = text_cleaning(title)
    if title:
        return re.sub(pattern_prefix, '', title).strip().capitalize()
    return None


def text_cleaning(text: str) -> str | None:
    """ Удаляет из строки служебные символы и лишние пробелы. """
    text = remove_wildcard(text)
    if text:
        return " ".join(text.split())
    return None


def split_code(src_code: str) -> tuple[str, ...] | None:
    """ Разбивает шифр на части. '4.1-2-10' -> ('4', '1', '2', '10')"""
    # if src_code:
    #     return tuple([x for x in re.split('[.-]', src_code) if x])
    # return tuple()
    return tuple([x for x in re.split('[.-]', src_code) if x]) if src_code else None


def split_code_int(src_code: str) -> tuple[int, ...] | None:
    """ Разбивает шифр на части из чисел. '4.1-2-10' -> (4, 1, 2, 10)"""
    return tuple(map(int, re.split('[.-]', src_code))) if src_code else None


def identify_item(src_code: str) -> tuple:
    """ Определяет по шифру категорию записи. """
    # ['5',       '5.1',          '5.1-1',    '5.1-1-1',      '5.1-1-1-0-1',  '5.1-1-1']
    # ['chapter', 'collection',   'section',  'subsection',   'table',        'quote']
    code = remove_wildcard(src_code)
    if code:
        length = len(split_code(code))
        match length:
            case 6:  # таблица
                extract = ('table',)
            case 4:  # раздел, расценка
                extract = ('subsection', 'quote')
            case 3:  # отдел
                extract = ('section',)
            case 2:  # сборник
                extract = ('collection',)
            case 1:  # глава
                extract = ('chapter',)
            case _:  # непонятно
                extract = tuple()
        return extract
    return tuple()


def check_code_item(src_code: str, item_name) -> bool:
    """ Проверяет, соответствует ли код указанному типу"""
    check_types = identify_item(src_code)
    if len(check_types) > 0 and check_types[0] == item_name:
        return True
    return False


# def _get_float_value(value: str) -> float:
#     try:
#         return float(str)
#     except ValueError:
#         return 0.0

def get_float_value(value: str) -> float:
    """ Конвертирует строку в число с плавающей точкой. """
    if value:
        value = value.replace(',', '.', 1)
        return float(value) if isfloat(value) else 0.0
    return 0.0


def get_integer_value(value: str) -> int:
    """ Конвертирует строку в целое число. """
    return int(value) if isint(value) else 0


def date_parse(value: str) -> str | None:
    """  Конвертирует строку в формат даты. """
    try:
        t = parser.parse(value)
        return t.strftime(r"%Y-%m-%d")
    except:
        return None


def convert_code_to_number(src_code: str) -> int | None:
    """ Преобразует шифр в число. '4.1-2-10' -> 41300000000 
        sys.maxsize = 9223372036854775807
    """
    if src_code and isinstance(src_code, str):
        MAX_FACTOR = 10**10
        splitted_code = split_code_int(src_code)
        result = 0
        for position, number in enumerate(splitted_code):
            ml = MAX_FACTOR/(100**position)
            result += number * ml
        return int(result) if result > 0 else None
    return None


# def convert_number_to_code(number: int) -> str | None:
#     """ Преобразует число d шифр. 41300000000 ==> '4.1-2-10' """
#     if number and isinstance(number, int):
#         MAX_FACTOR = 10**10
#     return None

if __name__ == "__main__":
    from icecream import ic

    codes = ('55.11-22-33-77-99', '1.1-2-8', '2', '4.1-2-10')
    for s in codes:
        x = convert_code_to_number(s)
        out = f"{s:18} ==> {x:15}"
        ic(out)

    # x = split_code_int('5.1-2-8')
    # ic(x)

    # x = split_code_int('5.1-1-1-0-1')
    # ic(x)
    


    # ic(date_parse('15.10.2024'))
    # date = '2024-01-11 00:00:00'
    # res = f"{date!r}: {date_parse(date)}"
    # ic(res)

    # date = '2024-02-27 00:00:00'
    # res = f"{date!r}: {date_parse(date)}"
    # ic(res)

    # item_prefix = r'^\s*Глава\s*((\d+)\.)*'
    # x_title = 'Глава 1. Средние сметные цены на материалы, изделия и конструкции'
    # xx = title_catalog_extraction(x_title, item_prefix)
    # ic(xx)



    # c = '5.1-2-8'
    # s = '5  . 1-1 .**-1-0-   1   '
    # ic(keep_just_numbers(s))
    # ic(keep_just_numbers_dots(s))
    #
    # ic(remove_wildcard(s))
    #
    # ic(split_code(c))
    #
    # ic(clear_code(s))
    # ic(clear_code(""))

    # print(items_data.keys())
    # print([x for x in items_data.keys()])
    # print(items_data['table'].prefix)
    # test = ['5', '5.1', '5.1-1', '5.1-1-1', '5.1-1-1-0-1', '5.1-1-1']
    # for x in test:
    #     print(identify_item(x))
