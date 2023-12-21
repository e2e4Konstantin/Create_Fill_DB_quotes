from collections import namedtuple


DirectoryItem = namedtuple(
    typename="DirectoryItem", field_names=['id', 'item_name', 'directory_name', 're_pattern', 're_prefix']
)
DirectoryItem.__annotations__ = {
    'id': int, 'item_name': str, 'directory_name': str, 're_pattern': str, 're_prefix': str
}

if __name__ == '__main__':
    from icecream import ic

    x = DirectoryItem(1, 'chapter', 'quotes', r'^\s*(\d+)\s*$', r'^\s*Глава\s*((\d+)\.)*')
    ic(x)
    ic(DirectoryItem.__annotations__)
