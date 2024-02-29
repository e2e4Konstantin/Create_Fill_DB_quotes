locations = ("home", "office")
paths=[r"c:\bin", r"f:\data"]

r = zip(locations, paths)
print(list(r))


d = {x[0]: x[1] for x in zip(locations, paths)}
print(d)
