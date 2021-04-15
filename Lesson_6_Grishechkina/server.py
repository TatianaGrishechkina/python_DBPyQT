"""Программа-сервер"""

import os
import socket
import sys
import select
import time
from common.jimbase import JIMBase
from common.json_messenger import JSONMessenger
import configparser
from decorator import Log, LOGGER
from descriptors import CheckPort, CheckHost
from metaclasses import ServerInspector
from threading import Thread
from server_database import ServerDB
from server_cli import run_server_cli
from server_config import ServerConfig
from server_gui import run_server_gui


class JIMServer(JIMBase, metaclass=ServerInspector):
    transport = None
    clients = []
    messages = []
    # Словарь, содержащий имена пользователей и соответствующие им сокеты.
    messengers = dict()
    listen_address = CheckHost()
    listen_port = CheckPort()
    database = None
    db_session = None
    on_connections_change = []

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

        # Открываем соединение с базой данных
        self.db_session = self.database.create_session()

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
                recv_data_lst, send_data_lst, err_lst = select.select(self.clients, self.clients, [], 5)
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
                    self.remove_client(client_with_message)
                except Exception as e:
                    LOGGER.info(f'Ошибка при получении сообщения: {e}')
                    self.remove_client(client_with_message)

        # Если есть сообщения, обрабатываем каждое.
        for i in self.messages:
            try:
                self.process_message(i, send_data_lst)
            except Exception:
                LOGGER.info(f'Связь с клиентом с именем {i[self.DESTINATION]} была потеряна')
                self.remove_client(self.messengers[i[self.DESTINATION]].sock)
        self.messages.clear()

    def remove_client(self, sock):
        self.clients.remove(sock)
        for name, messenger in self.messengers.items():
            if messenger.sock == sock:
                del self.messengers[name]
                self.db_session.user_logout(name)
                self.fire(self.on_connections_change)
                break

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
            client_name = message[self.USER][self.ACCOUNT_NAME]
            if client_name not in self.messengers.keys():
                self.db_session.user_login(client_name, self.listen_address, self.listen_port)
                response = {self.RESPONSE: 200}
                LOGGER.info(f'Cформирован ответ клиенту {response}')
                messenger.send_message(response)
                self.messengers[client_name] = messenger
                self.fire(self.on_connections_change)
            else:
                response = self.RESPONSE_400
                response[self.ERROR] = 'Имя пользователя уже занято.'
                messenger.send_message(response)
                self.clients.remove(messenger.sock)
                messenger.sock.close()

        elif message[self.ACTION] == self.MESSAGE \
                and self.TIME in message and self.MESSAGE_TEXT in message and self.DESTINATION in message \
                and self.SENDER in message:
            self.messages.append(message)
            self.db_session.process_message(self.SENDER, self.DESTINATION)
            LOGGER.info(f'Сообщение от клиента добавлено в очередь')

        # Если клиент выходит
        elif self.ACTION in message and message[self.ACTION] == self.EXIT and self.ACCOUNT_NAME in message:
            client_name = message[self.ACCOUNT_NAME]
            if self.messengers[client_name] == messenger:
                self.db_session.user_logout(message[self.ACCOUNT_NAME])
                LOGGER.info(f'Клиент {message[self.ACCOUNT_NAME]} корректно отключился от сервера.')
                self.clients.remove(self.messengers[message[self.ACCOUNT_NAME]])
                self.messengers[message[self.ACCOUNT_NAME]].close()
                del self.messengers[message[self.ACCOUNT_NAME]]
                self.fire(self.on_connections_change)

        # Если это запрос контакт-листа
        elif self.ACTION in message and message[self.ACTION] == self.GET_CONTACTS and self.USER in message and \
                self.messengers[message[self.USER]].sock == messenger.sock:
            response = self.RESPONSE_202
            response[self.LIST_INFO] = self.db_session.get_contacts(message[self.USER])
            messenger.send_message(response)

        # Если это добавление контакта
        elif self.ACTION in message and message[self.ACTION] == self.ADD_CONTACT and self.ACCOUNT_NAME in message \
                and self.USER in message and self.messengers[message[self.USER]].sock == messenger.sock:
            self.db_session.add_contact(message[self.USER], message[self.ACCOUNT_NAME])
            response = {self.RESPONSE: 200}
            messenger.send_message(response)

        # Если это удаление контакта
        elif self.ACTION in message and message[self.ACTION] == self.REMOVE_CONTACT and self.ACCOUNT_NAME in message \
                and self.USER in message and self.messengers[message[self.USER]].sock == messenger.sock:
            self.db_session.remove_contact(message[self.USER], message[self.ACCOUNT_NAME])
            response = {self.RESPONSE: 200}
            messenger.send_message(response)

        # Если это запрос известных пользователей
        elif self.ACTION in message and message[self.ACTION] == self.USERS_REQUEST and self.ACCOUNT_NAME in message \
                and self.messengers[message[self.ACCOUNT_NAME]].sock == messenger.sock:
            response = self.RESPONSE_202
            response[self.LIST_INFO] = [user[0] for user in self.db_session.users_list()]
            messenger.send_message(response)

        # Иначе отдаём Bad request
        else:
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
        if message[self.DESTINATION] in self.messengers:
            if self.messengers[message[self.DESTINATION]].sock in listen_socks:
                messenger = self.messengers[message[self.DESTINATION]]
                messenger.send_message(message)
                LOGGER.info(f'Отправлено сообщение пользователю {message[self.DESTINATION]} '
                            f'от пользователя {message[self.SENDER]}.')
            else:
                raise ConnectionError
        else:
            LOGGER.error(
                f'Пользователь {message[self.DESTINATION]} не зарегистрирован на сервере, '
                f'отправка сообщения невозможна.')

    @staticmethod
    def fire(subscription_list):
        for callback in subscription_list:
            callback()


def server_func(my_server):
    while True:
        my_server.process()


def main():
    # Загрузка файла конфигурации сервера
    ini = configparser.ConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    ini.read(f"{dir_path}/{'server.ini'}")

    # Чтение конфигурации сервера
    config = ServerConfig()
    config.read_default()
    config.read_from_ini(ini)
    config.read_from_cmd()

    # Инициализация базы данных
    database = ServerDB(os.path.join(config.db_path, config.db_file))

    # Запуск сервера
    my_server = JIMServer()
    my_server.listen_address = config.address
    my_server.listen_port = config.port
    my_server.database = database
    my_server.start()
    print(f'Сервер запущен на порту {config.address}:{config.port}')

    # Запускаем поток обработки сообщений сервером
    server_thread = Thread(target=server_func, args=(my_server,))
    server_thread.daemon = True
    server_thread.start()

    # Запускаем пользовательский интерфейс
    #run_server_cli(server_thread, database)
    run_server_gui(my_server, config)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOGGER.critical(str(e))

