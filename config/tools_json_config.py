import json

from files_features import file_exist


def read_config_to_json(json_file_name: str) -> dict | None:
    """ Читает файл с конфигурацией. """
    if file_exist(json_file_name):
        with open(json_file_name, 'r', encoding='utf8') as file:
            data_loaded = json.load(file)
        return data_loaded if data_loaded else None
    return None


def write_config_to_json(json_file_name: str, config_data: dict) -> int:
    """ Записывает файл с конфигурацией. """
    if config_data:
        with open(json_file_name, 'w', encoding='utf8') as file:
            json.dump(config_data, file, sort_keys=True,
                      indent=4, ensure_ascii=False)
            return 0
    return 1


if __name__ == "__main__":
    from files_features import create_abspath_file
    from icecream import ic

    per_conf = {
        66: {'basic_id': 150719989, 'id': 66, 'title': 'Дополнение 68'},
        67: {'basic_id': 151427079, 'id': 67, 'title': 'Дополнение 69'},
        68: {'basic_id': 152472566, 'id': 68, 'title': 'Дополнение 70'},
        70: {'basic_id': 149000015, 'id': 70, 'title': 'Дополнение 67'},
        71: {'basic_id': 166954793, 'id': 71, 'title': 'Дополнение 71'},
    }

    pc = [
        {'basic_id': 150719989, 'id': 66, 'title': 'Дополнение 68'},
        {'basic_id': 151427079, 'id': 67, 'title': 'Дополнение 69'},
        {'basic_id': 152472566, 'id': 68, 'title': 'Дополнение 70'},
        {'basic_id': 149000015, 'id': 70, 'title': 'Дополнение 67'},
        {'basic_id': 166954793, 'id': 71, 'title': 'Дополнение 71'},
    ]

    path = r"C:\Users\kazak.ke\Documents\PythonProjects\Create_Fill_DB_quotes\config"
    file_name = "test_config.json"
    config_file = create_abspath_file(path, file_name)
    ic(config_file)

    write_config_to_json(config_file, per_conf)
    conf = read_config_to_json(config_file)
    ic(conf)
