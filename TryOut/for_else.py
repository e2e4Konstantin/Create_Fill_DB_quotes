from icecream import ic

# l2 = ["a", "b", "c"]
# l = [1, 3, 5, 7, 9]
# for i, v in enumerate(l):
#     ic(i, v)

# ic(i)
history = [205, 206, 207, 208, 209, 210]



header = [
    "code",
    "base_price",
    "index_num",
    "actual_price",
    "monitoring_index",
    "monitoring_price",
]


x = [*header[:2], *history, *header[2:]]
print(x)


