import itertools

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


def split_code(src_code: str=None) -> tuple[str, ...] | None:
    """ Разбивает шифр на части. '4.1-2-10' -> ('4', '1', '2', '10')"""
    # if src_code:
    #     return tuple([x for x in re.split('[.-]', src_code) if x])
    # return tuple()
    return tuple([x for x in re.split('[.-]', src_code) if x]) if src_code else None


def split_code_int(src_code: str=None) -> tuple[int, ...] | None:
    """ Разбивает шифр на части из чисел. '4.1-2-10' -> (4, 1, 2, 10)"""
    # return tuple(map(int, re.split('[.-]', src_code))) if src_code else None
    digit_string = split_code(src_code)
    ret = tuple(map(int, digit_string))
    return ret


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




# def code_to_number(src_code: str) -> int:
#     """ Преобразует шифр в число. '3.1-2-99 ' -> 3001002099000000000
#         sys.maxsize = 9223372036854775807
#         2**63-1 == 9223372036854775807 """
#     N = 3               # разрядов на группу
#     GROUP_NUMBER = 6    # количество групп
#     if src_code and isinstance(src_code, str):
#         factors = tuple([10**x for x in range((GROUP_NUMBER-1)*N, -N, -N)])
#         splitted_code = split_code_int(src_code)
#         if len(splitted_code) > 1:
#             pairs = list(itertools.zip_longest(
#                 splitted_code, factors, fillvalue=0))
#             return sum(map(lambda x: x[0]*x[1], pairs))
#         else:
#             return splitted_code[0] * factors[0] - 1
#     return 1


def code_to_number(src_code: str) -> int:
    """Преобразует шифр в число. '3.1-2-99 ' -> 3001002099000000000
    sys.maxsize = 9223372036854775807
    2**63-1 == 9223372036854775807"""
    N = 3  # разрядов на группу
    GROUP_NUMBER = 6  # количество групп
    if src_code and isinstance(src_code, str):
        factors = tuple([10**x for x in range((GROUP_NUMBER - 1) * N, -N, -N)])
        splitted_code = split_code_int(src_code)
        if len(splitted_code) > 1 and not all([x == 0 for x in splitted_code[1:]]):
            pairs = list(itertools.zip_longest(splitted_code, factors, fillvalue=0))
            return sum(map(lambda x: x[0] * x[1], pairs))
        else:
            return splitted_code[0] * factors[0] + len(splitted_code[1:])
    return 1


if __name__ == "__main__":
    from icecream import ic

    codes = (
        "1",
        "1.",
        "1.0",
        "1.0-0",
        "1.0-0-0",
        "1.0-0-0-0",
        "1.0-0-0-0-1",
    )

    # codes = ('1.', '10', '3', '3.0', '3.99', '3.1-99', '3.1-2-99',
    #          '3.1-2-999', '3.1-2-3-999', '3.1-2-3-4-999', '999.999-999-999-999-999', '999.888-777-666-555-444-333')
    for x in codes:
        print(f"{x:<15} {code_to_number(x)}")

    print(f"'0.0-0-0' {code_to_number('0.0-0-0')}")
    # for s in codes:
    #     x = code_to_number(s)
    #     out = f"{s:30} ==> {x:22} "
    #     ic(out)

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
