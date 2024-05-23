import math


def calculate_max_n(number):
    max_n = math.floor(math.log2(number))
    return max_n

def calculate_n2_series(num):
    """Возвращает список из n значений, где n - максимальное значение, которое может быть представлено с помощью
    заданным количеством битов. Например, calculate_n2_series(44) возвращает [5, 3, 2]. 44=2**5 + 2**3 + 2**2
    """
    result = []
    while num > 0:
        max_n = (num.bit_length() - 1)
        result.append(max_n)
        num -= 2 ** max_n
    return result

# number = 44

# r = calculate_n2_series(number)
# print(r)

# max_n = calculate_max_n(number)
# print(max_n)
# number = number - 2**max_n
# print(number)
# max_n = calculate_max_n(number)
# print(max_n)

# number.bit_length() - 1


# l = [5, 3, 2, 1]

# print(l[-3:])


pos = {
        "row_count": [1, "0"],
        "code":[2, "material.code"],
        "name": [3, "material.name"],
        "base_price": [4, "material.base_price"],
        #
        "history range": [5, None],
    }
l = {value[0]: value[1] for value in pos.values()}

r = [l[x] for x in sorted(list(l.keys()))]
print(r)
