"""Программа-сервер"""

import socket
import sys
import json
import select
from time import time
from errors import IncorrectDataRecivedError, NonDictInputError
from common.jimbase import JIMBase
from common.json_messenger import JSONMessenger
from decorator import Log, LOGGER
from descriptors import CheckPort, CheckHost
from metaclasses import ServerInspector


class JIMServer(JIMBase, metaclass=ServerInspector):
    transport = None
    clients = []
    messages = []
    # Словарь, содержащий имена пользователей и соответствующие им сокеты.
    names = dict()
    listen_address = CheckHost()
    listen_port = CheckPort()

    # @Log()
    def start(self):
        LOGGER.info(f'Запущен сервер, порт для подключений: {self.listen_port}, '
                    f'адрес с которого принимаются подключения: {self.listen_address}. '
                    f'Если адрес не указан, принимаются соединения с любых адресов.')
        # Готовим сокет
        self.transport = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.transport.bind((self.listen_address, self.listen_port))
        self.transport.settimeout(0.5)

        # Слушаем порт
        self.transport.listen(self.MAX_CONNECTIONS)

    # Основной цикл программы сервера
    def process(self):
        # Ждём подключения, если таймаут вышел, ловим исключение.
        try:
            client, client_address = self.transport.accept()
        except OSError:
            pass
        else:
            LOGGER.info(f'Установлено соединение с ПК {client_address}')
            self.clients.append(client)

        recv_data_lst = []
        send_data_lst = []
        err_lst = []

        # Проверяем на наличие ждущих клиентов
        try:
            if self.clients:
                recv_data_lst, send_data_lst, err_lst = select.select(self.clients, self.clients, [], 0)
        except OSError:
            pass

        # принимаем сообщения и если там есть сообщения,
        # кладём в словарь, если ошибка, исключаем клиента.
        if recv_data_lst:
            LOGGER.info('Есть сообщения от клиентов')
            for client_with_message in recv_data_lst:
                try:
                    messenger = JSONMessenger(client_with_message)
                    message = messenger.get_message()
                    self.process_client_message(messenger, message)
                except ConnectionResetError:
                    LOGGER.info(f'Клиент {client_with_message.getpeername()} '
                                f'отключился от сервера.')
                    self.clients.remove(client_with_message)
                except Exception as e:
                    LOGGER.info(f'Ошибка при получении сообщения: {e}')
                    self.clients.remove(client_with_message)

        # Если есть сообщения, обрабатываем каждое.
        for i in self.messages:
            try:
                self.process_message(i, send_data_lst)
            except Exception:
                LOGGER.info(f'Связь с клиентом с именем {i[self.DESTINATION]} была потеряна')
                self.clients.remove(self.names[i[self.DESTINATION]].sock)
                del self.names[i[self.DESTINATION]]
        self.messages.clear()

    @Log()
    def process_client_message(self, messenger, message):
        """
        Обработчик сообщений от клиентов, принимает словарь -
        сообщение от клинта, проверяет корректность,
        возвращает словарь-ответ для клиента

        :param messenger: экземпляр класса JSONMessenger
        :param message: словарь, полученный от клиента
        :return: возвращает словарь с ответом сервера
        """
        LOGGER.info(f'Разбор сообщения от клиента : {message}')

        if self.ACTION not in message:
            # Иначе отдаём Bad request
            messenger.send_message(self.BAD_REQUEST)
            return

        if message[self.ACTION] == self.PRESENCE \
                and self.TIME in message and self.USER in message and self.ACCOUNT_NAME in message[self.USER]:
            # {'action': 'presence', 'time': 1573760672.167031, 'user': {'account_name': 'Guest'}}
            response = {self.RESPONSE: 200}
            LOGGER.info(f'Cформирован ответ клиенту {response}')
            messenger.send_message(response)
            self.names[message[self.USER][self.ACCOUNT_NAME]] = messenger
            return
        elif message[self.ACTION] == self.MESSAGE \
                and self.TIME in message and self.MESSAGE_TEXT in message and self.DESTINATION in message \
                and self.SENDER in message:
            self.messages.append(message)
            LOGGER.info(f'Сообщение от клиента добавлено в очередь')
            return
        # Иначе отдаём Bad request
        messenger.send_message(self.BAD_REQUEST)

    @Log()
    def process_message(self, message, listen_socks):
        """
        Функция адресной отправки сообщения определённому клиенту. Принимает словарь сообщение,
        список зарегистрированых пользователей и слушающие сокеты. Ничего не возвращает.
        :param message:
        :param names:
        :param listen_socks:
        :return:
        """
        if message[self.DESTINATION] in self.names:
            if self.names[message[self.DESTINATION]].sock in listen_socks:
                messenger = self.names[message[self.DESTINATION]]
                messenger.send_message(message)
                LOGGER.info(f'Отправлено сообщение пользователю {message[self.DESTINATION]} '
                            f'от пользователя {message[self.SENDER]}.')
            else:
                raise ConnectionError
        else:
            LOGGER.error(
                f'Пользователь {message[self.DESTINATION]} не зарегистрирован на сервере, '
                f'отправка сообщения невозможна.')


def main():
    """
    Загрузка параметров командной строки, если нет параметров, то задаём значения по умоланию.
    Сначала обрабатываем порт:
    server.py -p 8079 -a 192.168.1.2
    """
    try:
        if '-p' in sys.argv:
            listen_port = sys.argv[sys.argv.index('-p') + 1]
        else:
            listen_port = JIMBase.DEFAULT_PORT
    except IndexError:
        LOGGER.critical('После параметра -\'p\' необходимо указать номер порта.')
        sys.exit(1)

    # Затем загружаем какой адрес слушать

    try:
        if '-a' in sys.argv:
            listen_address = sys.argv[sys.argv.index('-a') + 1]
        else:
            listen_address = JIMBase.DEFAULT_IP_ADDRESS

    except IndexError:
        LOGGER.critical('После параметра \'a\'- необходимо указать адрес, который будет слушать сервер.')
        sys.exit(1)

    my_server = JIMServer()
    my_server.listen_address = listen_address
    my_server.listen_port = int(listen_port)
    my_server.start()

    while True:
        my_server.process()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOGGER.critical(str(e))

