import json

def read_config_to_json(json_file_name: str) -> dict:
    with open(json_file_name, 'w', encoding='utf8') as file:
        data_loaded = json.load(file)
    return data_loaded.get('config', None) if data_loaded else None


def write_config_to_json(json_file_name: str, config_data: dict) -> int:
    if config_data and isinstance(config_data, dict):
        with open(json_file_name, 'w', encoding='utf8') as file:
            config: dict[str: dict] = {}
            config['config'] = config_data
            json.dump(config, file, sort_keys=True, indent=4, ensure_ascii=False)
            return 0        
    return 1


if __name__ == "__main__":
    from files_features import create_abspath_file
    from config.const import CONFIG_FILE_NAME
    from icecream import ic

    per_conf = {
        66: {'basic_id': 150719989, 'id': 66, 'title': 'Дополнение 68'},
        67: {'basic_id': 151427079, 'id': 67, 'title': 'Дополнение 69'},
        68: {'basic_id': 152472566, 'id': 68, 'title': 'Дополнение 70'},
        70: {'basic_id': 149000015, 'id': 70, 'title': 'Дополнение 67'},
        71: {'basic_id': 166954793, 'id': 71, 'title': 'Дополнение 71'},
    }

    path = r"C:\Users\kazak.ke\Documents\PythonProjects\Create_Fill_DB_quotes\config"
    config_file = create_abspath_file(path, CONFIG_FILE_NAME)
    ic(config_file)

    write_config_to_json(config_file, per_conf)
    conf = read_config_to_json(config_file)
    ic(conf)
