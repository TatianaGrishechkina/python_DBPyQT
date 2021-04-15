"""Программа-сервер"""

import os
import socket
import sys
import select
import time
import hmac
import binascii
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
            self.authorize_user(messenger, message)

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

        # Если это запрос публичного ключа пользователя
        elif self.ACTION in message and message[self.ACTION] == self.PUBLIC_KEY_REQUEST and self.ACCOUNT_NAME in message:
            response = self.RESPONSE_511
            response[self.DATA] = self.db_session.get_pubkey(message[self.ACCOUNT_NAME])
            # может быть, что ключа ещё нет (пользователь никогда не логинился,
            # тогда шлём 400)
            if response[self.DATA]:
                try:
                    messenger.send_message(response)
                except OSError:
                    self.remove_client(messenger.sock)
            else:
                response = self.RESPONSE_400
                response[self.ERROR] = 'Нет публичного ключа для данного пользователя'
                try:
                    messenger.send_message(response)
                except OSError:
                    self.remove_client(messenger.sock)

        # Иначе отдаём Bad request
        else:
            messenger.send_message(self.BAD_REQUEST)

    def authorize_user(self, messenger, message):
        client_name = message[self.USER][self.ACCOUNT_NAME]
        if client_name in self.messengers.keys():
            response = self.RESPONSE_400
            response[self.ERROR] = 'Имя пользователя уже занято.'
            messenger.send_message(response)
            self.clients.remove(messenger.sock)
            messenger.sock.close()
            return

            # Проверяем что пользователь зарегистрирован на сервере.
        if not self.db_session.check_user(client_name):
            response = self.RESPONSE_400
            response[self.ERROR] = 'Пользователь не зарегистрирован.'
            try:
                LOGGER.debug(f'Unknown username, sending {response}')
                messenger.send_message(response)
            except OSError:
                pass
            self.clients.remove(messenger.sock)
            messenger.sock.close()
            return

        LOGGER.debug('Correct username, starting passwd check.')
        # Иначе отвечаем 511 и проводим процедуру авторизации
        # Словарь - заготовка
        message_auth = self.RESPONSE_511
        # Набор байтов в hex представлении
        random_str = binascii.hexlify(os.urandom(64))
        # В словарь байты нельзя, декодируем (json.dumps -> TypeError)
        message_auth[self.DATA] = random_str.decode('ascii')
        # Создаём хэш пароля и связки с рандомной строкой, сохраняем
        # серверную версию ключа
        hash = hmac.new(self.db_session.get_hash(client_name), random_str, 'MD5')
        digest = hash.digest()
        LOGGER.debug(f'Auth message = {message_auth}')
        try:
            # Обмен с клиентом
            messenger.send_message(message_auth)
            ans = messenger.get_message()
        except OSError as err:
            LOGGER.debug('Error in auth, data:', exc_info=err)
            self.clients.remove(messenger.sock)
            messenger.sock.close()
            return

        client_digest = binascii.a2b_base64(ans[self.DATA])
        # Если ответ клиента корректный, то сохраняем его в список
        # пользователей.
        if not (self.RESPONSE in ans and ans[self.RESPONSE] == 511 and hmac.compare_digest(
                digest, client_digest)):
            response = self.RESPONSE_400
            response[self.ERROR] = 'Неверный пароль.'
            try:
                messenger.send_message(response)
            except OSError:
                pass
            self.clients.remove(messenger.sock)
            messenger.sock.close()

        self.messengers[client_name] = messenger
        client_ip, client_port = messenger.sock.getpeername()
        try:
            messenger.send_message(self.RESPONSE_200)
        except OSError:
            self.remove_client(client_name)
        # добавляем пользователя в список активных и если у него изменился открытый ключ
        # сохраняем новый
        client_pubkey = message[self.USER][self.PUBLIC_KEY]
        self.db_session.user_login(client_name, client_ip, client_port, client_pubkey)
        self.fire(self.on_connections_change)

    @Log()
    def process_message(self, message, listen_socks):
        """
        Функция адресной отправки сообщения определённому клиенту. Принимает словарь сообщение,
        список зарегистрированых пользователей и слушающие сокеты. Ничего не возвращает.
        :param message:
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

    def service_update_lists(self):
        '''Метод реализующий отправки сервисного сообщения 205 клиентам.'''
        for messenger in self.messengers:
            try:
                messenger.send_message(self.RESPONSE_205)
            except OSError:
                self.remove_client(messenger.sock)

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

