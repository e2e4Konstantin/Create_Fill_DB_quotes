import csv


if __name__ == "__main__":

    with open(r"C:\Users\kazak.ke\Documents\Tmp\sqlite.csv", newline="") as file:
        data_history = csv.reader(file, delimiter=",", quotechar="|")
        print(data_history.__next__())
        history = set(x[1] for x in data_history)
        # print(history)
        print(len(history))

        #
    with open(r"C:\Users\kazak.ke\Documents\Tmp\postgre_table.csv", newline="", encoding="utf-8") as file:
        data_raw = csv.reader(file, delimiter=",", )
        print(data_raw.__next__())
        raw = set(x[1] for x in data_raw)
        # print(raw)
        print(len(raw))

    diff = raw.difference(history)
    print(len(diff))
    print(diff)
    # print("1.12-5-386" in diff)

    result = {
        "1.12-5-386",
        "1.1-1-971",
        "1.1-1-3049",
        "1.1-1-729",
        "1.1-1-922",
        "1.12-5-807",
        "1.12-5-385",
        "1.12-5-820",
        "1.12-5-839",
        "1.12-5-388",
        "1.12-5-810",
        "1.1-1-4076",
        "1.12-5-858",
        "1.1-1-975",
        "1.1-1-4074",
        "1.1-1-4077",
        "1.12-5-521",
        "1.12-5-380",
        "1.12-5-814",
        "1.12-5-840",
        "1.12-5-841",
        "1.1-1-3300",
        "1.1-1-3046",
        "1.1-1-3697",
        "1.1-1-1906",
        "1.12-5-824",
        "1.1-1-1904",
        "1.12-5-751",
        "1.12-5-793",
        "1.12-5-387",
        "1.12-5-822",
        "1.1-1-4036",
        "1.12-5-860",
        "1.1-1-4051",
        "1.1-1-1898",
        "1.12-5-837",
        "1.1-1-4058",
        "1.1-1-3831",
        "1.1-1-916",
        "1.12-5-834",
        "1.12-5-389",
        "1.12-5-801",
        "1.1-1-3280",
        "1.5-4-1316",
        "1.1-1-3363",
        "1.1-1-4346",
        "1.1-1-4032",
        "1.12-5-804",
        "1.1-1-3297",
        "1.12-5-836",
        "1.12-5-845",
        "1.1-1-3281",
        "1.1-1-3051",
        "1.1-1-1010",
        "1.12-5-791",
        "1.12-5-848",
        "1.1-1-3591",
        "1.12-5-795",
        "1.1-1-3583",
        "1.1-1-4044",
        "1.1-1-1011",
        "1.1-1-3044",
        "1.1-1-662",
        "1.1-1-3368",
        "1.12-5-847",
        "1.12-5-752",
        "1.7-7-143",
        "1.12-5-826",
        "1.1-1-4050",
        "1.12-5-813",
        "1.1-1-3047",
        "1.12-5-808",
        "1.1-1-917",
        "1.12-5-859",
        "1.12-5-830",
        "1.1-1-4061",
        "1.12-5-800",
        "1.1-1-3284",
        "1.1-1-667",
        "1.12-5-383",
        "1.1-1-4347",
        "1.12-5-831",
        "1.1-1-3279",
        "1.12-5-815",
        "1.1-1-53",
        "1.1-1-3658",
        "1.1-1-4040",
        "1.12-5-799",
        "1.12-5-381",
        "1.12-5-802",
        "1.1-1-3048",
        "1.1-1-3595",
        "1.1-1-3372",
        "1.1-1-3301",
        "1.12-5-794",
        "1.12-5-832",
        "1.12-5-522",
        "1.1-1-728",
        "1.12-5-818",
        "1.12-5-803",
        "1.12-5-828",
        "1.1-1-1899",
        "1.7-7-151",
        "1.12-5-829",
        "1.12-5-378",
        "1.1-1-125",
        "1.12-5-382",
        "1.12-5-856",
        "1.12-5-377",
        "1.12-5-852",
        "1.1-1-663",
        "1.7-7-148",
        "1.12-5-524",
        "1.1-1-3302",
        "1.12-5-855",
        "1.12-5-384",
        "1.1-1-665",
        "1.12-5-838",
        "1.1-1-3293",
        "1.12-5-816",
        "1.12-5-849",
        "1.12-5-817",
        "1.1-1-1912",
        "1.12-5-842",
        "1.1-1-3371",
        "1.1-1-4345",
        "1.7-7-150",
        "1.1-1-3045",
        "1.1-1-4060",
        "1.1-1-3282",
        "1.1-1-3295",
        "1.1-1-973",
        "1.1-1-54",
        "1.1-1-3322",
        "1.12-5-812",
        "1.1-1-8089",
        "1.1-1-4344",
        "1.12-5-823",
        "1.1-1-8084",
        "1.1-1-3659",
        "1.1-1-3660",
        "1.1-1-4112",
        "1.1-1-661",
        "1.12-5-750",
        "1.12-5-805",
        "1.12-5-790",
        "1.1-1-4089",
        "1.12-5-853",
        "1.5-4-1317",
        "1.12-5-796",
        "1.12-5-798",
        "1.1-1-8086",
        "1.12-5-843",
        "1.1-1-3285",
        "1.7-7-147",
        "1.7-7-144",
        "1.1-1-1911",
        "1.1-1-8085",
        "1.12-5-806",
        "1.1-1-3359",
        "1.12-5-809",
        "1.7-7-145",
        "1.12-5-825",
        "1.1-1-3361",
        "1.12-5-835",
        "1.12-5-811",
        "1.1-1-1009",
        "1.1-1-3362",
        "1.12-5-523",
        "1.1-1-3370",
        "1.1-1-1901",
        "1.1-1-920",
        "1.1-1-4062",
        "1.12-5-379",
        "1.1-1-972",
        "1.12-5-846",
        "1.1-1-974",
        "1.1-1-3294",
        "1.1-1-8087",
        "1.1-1-4073",
        "1.1-1-4075",
        "1.1-1-4057",
        "1.1-1-8090",
        "1.1-1-1905",
        "1.12-5-850",
        "1.7-7-146",
        "1.12-5-851",
        "1.12-5-827",
        "1.1-1-1903",
        "1.12-5-792",
        "1.1-1-3360",
        "1.12-5-819",
        "1.1-1-3369",
        "1.1-1-4348",
        "1.1-1-8083",
        "1.5-4-1318",
        "1.1-1-3582",
        "1.12-5-861",
        "1.1-1-3283",
        "1.12-5-854",
        "1.1-1-4059",
        "1.12-5-844",
        "1.12-5-797",
        "1.1-1-3830",
        "1.7-7-152",
    }