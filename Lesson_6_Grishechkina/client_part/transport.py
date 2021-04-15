"""Основное окошко клиента"""
import sys
import json
import socket
import time
from errors import ReqFieldMissingError, NonDictInputError
from common.jimbase import JIMBase
from common.json_messenger import JSONMessenger
from decorator import Log, LOGGER
from threading import Lock, Thread
from client_part.client_database import ClientDB
from PyQt5.QtCore import pyqtSignal, QObject
sys.path.append('../')


class JIMClient(JIMBase, QObject):
    lock = Lock()
    transport = None
    messenger = None
    server_address = ''
    client_name = ''
    database = None
    db_session = None
    # Сигналы новое сообщение и потеря соединения
    new_message = pyqtSignal(str)
    connection_lost = pyqtSignal()

    @Log()
    def message_from_server(self, message):
        """Функция - обработчик сообщений других пользователей, поступающих с сервера"""

        # Если это подтверждение чего-либо
        if self.RESPONSE in message:
            if message[self.RESPONSE] == 200:
                LOGGER.debug(f'Получен ответ от сервера!')
            elif message[self.RESPONSE] == 400:
                raise Exception(f'{message[self.ERROR]}')
            else:
                LOGGER.debug(f'Принят неизвестный код подтверждения {message[self.RESPONSE]}')

        elif self.ACTION in message and message[self.ACTION] == self.MESSAGE and \
                self.SENDER in message and self.MESSAGE_TEXT in message:
            LOGGER.info(f'Получено сообщение от пользователя '
                        f'{message[self.SENDER]}:\n{message[self.MESSAGE_TEXT]}')
            # Захватываем работу с базой данных и сохраняем в неё сообщение
            #with self.database.lock:
            try:
                self.db_session.save_message(message[JIMBase.SENDER], self.client_name,
                                           message[JIMBase.MESSAGE_TEXT])
            except:
                LOGGER.error('Ошибка взаимодействия с базой данных')
            self.new_message.emit(message[self.SENDER])

        else:
            LOGGER.error(f'Получено некорректное сообщение с сервера: {message}')

    @Log()
    def send_message(self, text, dest):
        self.messenger.send_message(self.create_message(text, self.client_name, dest))
        # Сохраняем сообщения для истории
        #with self.database.lock:
        self.db_session.save_message(self.client_name, dest, text)

    # Функция создаёт словарь с сообщением о выходе.
    def create_exit_message(self):
        return {
            self.ACTION: self.EXIT,
            self.TIME: time.time(),
            self.ACCOUNT_NAME: self.client_name
        }

    @classmethod
    @Log()
    def create_presence(cls, account_name='Guest'):
        """
        Функция генерирует запрос о присутствии клиента
        :param account_name: по умолчанию = Guest
        :return: словарь
        """
        LOGGER.debug(f'Сформировано {cls.PRESENCE} сообщение для пользователя {account_name}')
        # {'action': 'presence', 'time': 1573760672.167031, 'user': {'account_name': 'Guest'}}
        return {
            cls.ACTION: cls.PRESENCE,
            cls.TIME: time.time(),
            cls.USER: {
                cls.ACCOUNT_NAME: account_name
            }
        }

    # Функция запрос контакт листа
    def contacts_list_request(self, name):
        print(f'contacts_list_request {name}')
        LOGGER.debug(f'Запрос контакт листа для пользователся {name}')
        req = {
            self.ACTION: self.GET_CONTACTS,
            self.TIME: time.time(),
            self.USER: name
        }
        LOGGER.debug(f'Сформирован запрос {req}')
        self.messenger.send_message(req)
        ans = self.messenger.get_message()
        LOGGER.debug(f'Получен ответ {ans}')
        if self.RESPONSE in ans and ans[self.RESPONSE] == 202:
            return ans[self.LIST_INFO]
        else:
            raise RuntimeError

    # Функция добавления пользователя в контакт лист
    def add_contact(self, contact):
        print(f'add_contact {contact}')
        LOGGER.debug(f'Создание контакта {contact}')
        req = {
            self.ACTION: self.ADD_CONTACT,
            self.TIME: time.time(),
            self.USER: self.client_name,
            self.ACCOUNT_NAME: contact
        }
        self.messenger.send_message(req)
        ans = self.messenger.get_message()
        if self.RESPONSE in ans and ans[self.RESPONSE] == 200:
            pass
        else:
            raise RuntimeError('Ошибка создания контакта')

    # Функция запроса списка известных пользователей
    def user_list_request(self, username):
        print(f'user_list_request {username}')
        LOGGER.debug(f'Запрос списка известных пользователей {username}')
        req = {
            self.ACTION: self.USERS_REQUEST,
            self.TIME: time.time(),
            self.ACCOUNT_NAME: username
        }
        with self.lock:
            self.messenger.send_message(req)
            ans = self.messenger.get_message()
        if self.RESPONSE in ans and ans[self.RESPONSE] == 202:
            return ans[self.LIST_INFO]
        else:
            LOGGER.error('Не удалось обновить список известных пользователей.')
            raise RuntimeError

    # Функция удаления пользователя из контакт листа
    def remove_contact(self, contact):
        print(f'remove_contact {contact}')
        LOGGER.debug(f'Создание контакта {contact}')
        req = {
            self.ACTION: self.REMOVE_CONTACT,
            self.TIME: time.time(),
            self.USER: self.client_name,
            self.ACCOUNT_NAME: contact
        }
        self.messenger.send_message(req)
        ans = self.messenger.get_message()
        if self.RESPONSE in ans and ans[self.RESPONSE] == 200:
            pass
        else:
            raise RuntimeError('Ошибка удаления клиента')
        print('Удачное удаление')

    # Функция инициализатор базы данных. Запускается при запуске, загружает данные в базу с сервера.
    def database_load(self, username):
        self.database = ClientDB(username)
        self.db_session = self.database.create_session()

        # Загружаем список известных пользователей
        try:
            users_list = self.user_list_request(username)
        except RuntimeError:
            LOGGER.error('Ошибка запроса списка известных пользователей.')
        else:
            self.db_session.add_users(users_list)

        # Загружаем список контактов
        try:
            contacts_list = self.contacts_list_request(username)
        except RuntimeError:
            LOGGER.error('Ошибка запроса списка контактов.')
        else:
            for contact in contacts_list:
                self.db_session.add_contact(contact)

    # @Log()
    def start(self, server_address, server_port, client_name):
        # Инициализация сокета и обмен
        try:
            self.server_address = server_address
            self.client_name = client_name
            self.transport = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.transport.connect((server_address, server_port))
            self.messenger = JSONMessenger(self.transport)
            message_to_server = self.create_presence(self.client_name)
            self.messenger.send_message(message_to_server)
            self.message_from_server(self.messenger.get_message())
        except (ValueError, json.JSONDecodeError):
            LOGGER.error('Не удалось декодировать полученную Json строку.')
            print('Не удалось декодировать сообщение сервера.')
        except NonDictInputError:
            LOGGER.error(f'Аргумент функции должен быть словарем!')
        except ReqFieldMissingError as missing_error:
            LOGGER.error(f'В ответе сервера отсутствует необходимое поле '
                         f'{missing_error.missing_field}')
        except ConnectionRefusedError:
            LOGGER.critical(f'Не удалось подключиться к серверу {server_address}:{server_port}, '
                            f'конечный компьютер отверг запрос на подключение.')
            self.connection_lost.emit()
        except TimeoutError:
            LOGGER.error('Попытка установить соединение была безуспешной, '
                         'т.к. от другого компьютера за требуемое время не получен нужный отклик.')
            print('Попытка установить соединение была безуспешной, '
                  'т.к. от другого компьютера за требуемое время не получен нужный отклик.')
            self.connection_lost.emit()

        # Если соединение с сервером установлено корректно,
        # запускаем клиенский процесс приёма сообщний
        receiver = Thread(target=self.listen_func)
        receiver.daemon = True
        receiver.start()

    @Log()
    def stop(self):
        with self.lock:
            self.messenger.send_message(self.create_exit_message())
            self.transport.close()
            LOGGER.info('Завершение работы по команде пользователя.')

    def listen_func(self):
        running = True
        # Поток приёма:
        while running:
            time.sleep(1)
            with self.lock:
                try:
                    self.transport.settimeout(0.5)
                    message = self.messenger.get_message()
                except OSError as err:
                    if err.errno:
                        LOGGER.critical(f'Потеряно соединение с сервером.')
                        running = False
                        self.connection_lost.emit()
                except (ConnectionResetError, ConnectionError, ConnectionAbortedError):
                    LOGGER.error(f'Соединение с сервером {self.server_address} было потеряно.')
                    sys.exit(1)
                # Если сообщение получено, то вызываем функцию обработчик:
                else:
                    self.message_from_server(message)
                finally:
                    self.transport.settimeout(5)
