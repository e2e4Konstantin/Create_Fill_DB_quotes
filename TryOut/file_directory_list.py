import os
from operator import methodcaller



def get_file_list(directory):
    file_list = []
    for file in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, file)):
            file_list.append(file)
    return file_list


directory = r"C:\Users\kazak.ke\Documents\АИС_Выгрузка\Мониторинг"
file_list = get_file_list(directory)
print(file_list)




starts_with_b = methodcaller("startswith", "m")
filter_files = filter(starts_with_b, file_list)
print(list(filter_files))