

def date_parse(s):
    """  Converts a string to a date """
    try:
        t = parser.parse(s, parser.parserinfo(dayfirst=True))
        return t.strftime('%Y-%m-%d')
    except:
        return None

x = date_parse('15.10.2024')
print(x)
