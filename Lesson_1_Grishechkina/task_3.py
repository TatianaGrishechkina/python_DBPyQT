"""
Написать функцию host_range_ping_tab(), возможности которой основаны на функции из примера 2.
Но в данном случае результат должен быть итоговым по всем ip-адресам, представленным в табличном формате
(использовать модуль tabulate). Таблица должна состоять из двух колонок
"""
from tabulate import tabulate
from task_2 import host_range_ping


def host_range_ping_tab():
    """
    Функция для того, чтобы красиво выдавать функцию из task_2
    :return: Выдает данные в виде таблицы
    """
    my_host_dict = host_range_ping()
    print(tabulate([my_host_dict], headers='keys', tablefmt="pipe"))


if __name__ == '__main__':
    host_range_ping_tab()

