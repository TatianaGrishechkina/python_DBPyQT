"""
1. Написать функцию host_ping(), в которой с помощью утилиты ping
будет проверяться доступность сетевых узлов.
Аргументом функции является список, в котором каждый сетевой узел
должен быть представлен именем хоста или ip-адресом.
В функции необходимо перебирать ip-адреса и проверять
их доступность с выводом соответствующего сообщения
(«Узел доступен», «Узел недоступен»). При этом ip-адрес
сетевого узла должен создаваться с помощью функции ip_address().
"""
from ipaddress import ip_address
from subprocess import Popen, PIPE

host_list = ['127.0.0.1', 'google.com', 'google.ru', 'ya.ru', '192.0.2.1']


def host_ping(my_list, timeout=1000, requests=1):
    """
    Функция пингует IP
    :param my_list: список IP
    :param timeout: параметр для команды IP - сколько ждать ответа
    :param requests: параметр для команды IP - сколько запросов слать
    :return: возвращает словарик массивами у каких адресов прошел пинг, а у каких - нет
    """
    res = {'Пинг проходит': '', 'Пинг не проходит': ''}
    print('Начинаем проверочку: ')
    for host in my_list:
        try:
            network = ip_address(host)
        except ValueError:
            # print(f'{host} - не IP!') # если нужен вывод IP это или нет
            network = host
        my_ping = Popen(f'ping {network} -w {timeout} -n {requests}', shell=False, stdout=PIPE, stderr=PIPE)
        my_wait = my_ping.wait()
        if my_wait == 0:
            print(f'{host} - Хост доступен!')
            res['Пинг проходит'] += f'{str(network)}\n'
        else:
            print(f'{host} - Хост недоступен!')
            res['Пинг не проходит'] += f'{str(network)}\n'
    return res


if __name__ == '__main__':
    host_ping(host_list)
