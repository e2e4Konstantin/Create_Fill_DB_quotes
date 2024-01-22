from icecream import ic

# def testMultilineValueWrapped(self):
#         # Multiline values are line wrapped.
#         multilineStr = 'line1\nline2'
#         with disableColoring(), captureStandardStreams() as (out, err):
#             ic(multilineStr)
#         pair = parseOutputIntoPairs(out, err, 2)[0][0]
#         assert pair == ('multilineStr', ic.argToStringFunction(multilineStr))

multilineStr = 'line1\nline2'
ic(multilineStr)

