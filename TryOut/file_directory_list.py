import os


def get_file_list(directory):
    file_list = []
    for file in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, file)):
            file_list.append(file)
    return file_list


directory = r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Мониторинг"
file_list = get_file_list(directory)
print(file_list)