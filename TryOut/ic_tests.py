from icecream import ic

# def testMultilineValueWrapped(self):
#         # Multiline values are line wrapped.
#         multilineStr = 'line1\nline2'
#         with disableColoring(), captureStandardStreams() as (out, err):
#             ic(multilineStr)
#         pair = parseOutputIntoPairs(out, err, 2)[0][0]
#         assert pair == ('multilineStr', ic.argToStringFunction(multilineStr))

# multilineStr = 'line1\nline2'
# ic(multilineStr)


added = 158
updated = 5
bug = 200 - (added + updated)
name = "collection"
message = (
    f"{name:>12} : добавлено: {added:>5}, обновлено: {updated:>5}, ошибки: {bug:>5}"
)
ic(message)

num = 2323
neg = -3232
print(f"{num:>+5} | {num:> 5} | {neg:>+5} | {neg:> 5} |")
