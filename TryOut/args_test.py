

target_item = ( 'clip', 'Catalog')

def foo(*args):
    print(args)


foo('1')

foo(*target_item)