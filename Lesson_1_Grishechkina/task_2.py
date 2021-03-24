"""
2. Написать функцию host_range_ping() для перебора ip-адресов из заданного диапазона.
Меняться должен только последний октет каждого адреса.
По результатам проверки должно выводиться соответствующее сообщение.
"""
from ipaddress import ip_address
from task_1 import host_ping


def host_range_ping():
    """
    Функция формирует диапазон IP адресов и возвращает использование функции с task_1
    :return:
    """
    while True:  # прописываем, чтобы не прекращало ничего работать пока не введем IP корректно
        try:
            my_start_ip = input('Введите IP адрес, с которого начнем проверять:  >>  ')
            start_ip = my_start_ip.split('.')  # разделяю каждое число в IP, разделитель - точка
            max_oct = 254  # максимальное число в октете
            min_oct = 0  # минимальное число в октете
            oct_in_ip = 4  # кол-во октетов в ip

            # далее перевожу все числа в int
            start_ip[0] = int(start_ip[0])
            start_ip[1] = int(start_ip[1])
            start_ip[2] = int(start_ip[2])
            start_ip[3] = int(start_ip[3])
            # смотрю, что каждое число не больше 254 и не меньше 0
            i = 0
            if len(start_ip) == oct_in_ip:
                while i < oct_in_ip:  # !!!
                    if not (start_ip[i] <= max_oct and start_ip[i] >= min_oct):
                        break
                    i += 1
                else:
                    break
                raise ValueError
            else:
                raise ValueError
        except ValueError:
            print('Введите правильно IP!')

    while True:  # прописываем, чтобы не прекращало ничего работать пока не введем диапазон корректно
        try:
            end_ip = int(input('Сколько адресов перебрать?  >>  '))
            # смотрим, чтобы не пришлось переходить на предыдущий октет, все изменения только в последнем!
            if start_ip[3] + end_ip > max_oct + 1:
                print(f'Меняется только последний октет, пожалуйста введите число не больше '
                      f'{max_oct - start_ip[3] + 1}')
            else:
                break
        except ValueError:
            print('Тут должно быть число!')

    # Составляем список хостов, используем range
    host_list = []
    [host_list.append(ip_address(my_start_ip)+x) for x in range(end_ip)]

    # возвращаем функцию, в которой пингуем все узлы с нашего сформированного списка
    return host_ping(host_list)


if __name__ == '__main__':
    host_range_ping()
