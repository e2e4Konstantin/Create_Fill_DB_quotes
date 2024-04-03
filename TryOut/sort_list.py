periods = [
    {
        "id": 72,
        "title": "Дополнение 72",
        "basic_id": 167085727,
        "supplement": 72,
        "index": 210,
    },
    {
        "id": 71,
        "title": "Дополнение 71",
        "basic_id": 166954793,
        "supplement": 71,
        "index": 207,
    },
    {
        "id": 68,
        "title": "Дополнение 70",
        "basic_id": 152472566,
        "supplement": 70,
        "index": 204,
    },
    {
        "id": 67,
        "title": "Дополнение 69",
        "basic_id": 151427079,
        "supplement": 69,
        "index": 201,
    },
]

from pprint import pprint

pprint(periods)
periods.sort(reverse=False, key=lambda x: x["supplement"])
pprint(periods)

