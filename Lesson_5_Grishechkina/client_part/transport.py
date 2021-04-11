"""Основное окошко клиента"""
import sys
import json
import socket
import time
sys.path.append('../')
from errors import ReqFieldMissingError, NonDictInputError
from common.jimbase import JIMBase
from common.json_messenger import JSONMessenger
from decorator import Log, LOGGER
from threading import Lock
from metaclasses import ClientInspector
from client_part.client_database import ClientDB
from PyQt5.QtCore import pyqtSignal, QObject

# Объект блокировки сокета и работы с базой данных
sock_lock = Lock()
database_lock = Lock()


class JIMClient(JIMBase, QObject):
    transport = None
    messenger = None
    server_address = ''
    client_name = ''
    database = None
    # Сигналы новое сообщение и потеря соединения
    new_message = pyqtSignal(str)
    connection_lost = pyqtSignal()

    @Log()
    def message_from_server(self):
        """Функция - обработчик сообщений других пользователей, поступающих с сервера"""
        message = self.messenger.get_message()
        if self.ACTION in message and message[self.ACTION] == self.MESSAGE and \
                self.SENDER in message and self.MESSAGE_TEXT in message:
            LOGGER.info(f'Получено сообщение от пользователя '
                        f'{message[self.SENDER]}:\n{message[self.MESSAGE_TEXT]}')
            self.new_message.emit(message[self.SENDER])
            # Захватываем работу с базой данных и сохраняем в неё сообщение
            with database_lock:
                try:
                    self.database.save_message(message[JIMBase.SENDER], self.client_name,
                                               message[JIMBase.MESSAGE_TEXT])
                except:
                    LOGGER.error('Ошибка взаимодействия с базой данных')
            return message[self.SENDER], message[self.MESSAGE_TEXT]
        else:
            LOGGER.error(f'Получено некорректное сообщение с сервера: {message}')
            return None, None

    @Log()
    def send_message(self, text, dest):
        self.messenger.send_message(self.create_message(text, self.client_name, dest))
        # Сохраняем сообщения для истории
        with database_lock:
            self.database.save_message(self.client_name, dest, text)

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

    @classmethod
    @Log()
    def process_ans(cls, message):
        """
        Функция разбирает ответ сервера
        :param message: словарь - сообщение от сервера
        :return: ответ от сервера
        """
        LOGGER.debug(f'Разбор сообщения от сервера: {message}')

        if cls.RESPONSE in message:
            if message[cls.RESPONSE] == 200:
                return 'Получен ответ от сервера!'
            return f'400 : {message[cls.ERROR]}'
        raise ValueError

    # Функция запрос контакт листа
    def contacts_list_request(self, name):
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
        LOGGER.debug(f'Запрос списка известных пользователей {username}')
        req = {
            self.ACTION: self.USERS_REQUEST,
            self.TIME: time.time(),
            self.ACCOUNT_NAME: username
        }
        self.messenger.send_message(req)
        ans = self.messenger.get_message()
        if self.RESPONSE in ans and ans[self.RESPONSE] == 202:
            return ans[self.LIST_INFO]
        else:
            raise RuntimeError

    # Функция удаления пользователя из контакт листа
    def remove_contact(self, contact):
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

        # Загружаем список известных пользователей
        try:
            users_list = self.user_list_request(username)
        except RuntimeError:
            LOGGER.error('Ошибка запроса списка известных пользователей.')
        else:
            self.database.add_users(users_list)

        # Загружаем список контактов
        try:
            contacts_list = self.contacts_list_request(username)
        except RuntimeError:
            LOGGER.error('Ошибка запроса списка контактов.')
        else:
            for contact in contacts_list:
                self.database.add_contact(contact)

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
            answer = self.process_ans(self.messenger.get_message())
            LOGGER.info(f'Принят ответ от сервера {answer}')
            print(answer)
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

    @Log()
    def stop(self):
        with sock_lock:
            self.messenger.send_message(self.create_exit_message())
            self.transport.close()
            LOGGER.info('Завершение работы по команде пользователя.')


def sender_func(my_cl):
    # Поток отправки и взаимодействия с пользователем
    try:
        while True:
            print_help()
            while True:
                command = input('Введите команду: ')
                # Если отправка сообщения - соответствующий метод
                if command == 'message':
                    sender_message(my_cl)

                # Вывод помощи
                elif command == 'help':
                    print_help()

                # Выход. Отправляем сообщение серверу о выходе.
                elif command == 'exit':
                    try:
                        my_cl.stop()
                    except:
                        pass
                    print('Спасибо за использование нашего сервиса!')
                    LOGGER.info('Завершение работы по команде пользователя.')
                    # Задержка неоходима, чтобы успело уйти сообщение о выходе
                    time.sleep(0.5)
                    sys.exit(0)

                # Список контактов
                elif command == 'contacts':
                    with database_lock:
                        contacts_list = my_cl.database.get_contacts()
                    for contact in contacts_list:
                        print(contact)

                # Редактирование контактов
                elif command == 'edit':
                    edit_contacts(my_cl)

                # история сообщений.
                elif command == 'history':
                    print_history(my_cl)

                else:
                    print('Команда не распознана, попробойте снова. help - вывести поддерживаемые команды.')

    except (ConnectionResetError, ConnectionError, ConnectionAbortedError):
        LOGGER.error(f'Соединение с сервером {my_cl.server_address} было потеряно.')
        sys.exit(1)


# Функция выводящяя справку по использованию.
def print_help():
    print('Поддерживаемые команды:')
    print('message - отправить сообщение. Кому и текст будет запрошены отдельно.')
    print('history - история сообщений')
    print('contacts - список контактов')
    print('edit - редактирование списка контактов')
    print('help - вывести подсказки по командам')
    print('exit - выход из программы')


# Функция выводящяя историю сообщений
def print_history(my_cl):
    ask = input('Показать входящие сообщения - in, исходящие - out, все - просто Enter: ')
    with database_lock:
        if ask == 'in':
            history_list = my_cl.database.get_history(to_who=my_cl.client_name)
            for message in history_list:
                print(f'\nСообщение от пользователя: {message[0]} от {message[3]}:\n{message[2]}')
        elif ask == 'out':
            history_list = my_cl.database.get_history(from_who=my_cl.client_name)
            for message in history_list:
                print(f'\nСообщение пользователю: {message[1]} от {message[3]}:\n{message[2]}')
        else:
            history_list = my_cl.database.get_history()
            for message in history_list:
                print(f'\nСообщение от пользователя: {message[0]}, пользователю {message[1]} от {message[3]}\n{message[2]}')


# Функция изменеия контактов
def edit_contacts(my_cl):
    ans = input('Для удаления введите del, для добавления add: ')
    if ans == 'del':
        edit = input('Введите имя удаляемного контакта: ')
        with database_lock:
            if my_cl.database.check_contact(edit):
                my_cl.database.del_contact(edit)
            else:
                LOGGER.error('Попытка удаления несуществующего контакта.')
    elif ans == 'add':
        # Проверка на возможность такого контакта
        edit = input('Введите имя создаваемого контакта: ')
        if my_cl.database.check_user(edit):
            with database_lock:
                my_cl.database.add_contact(edit)
            with sock_lock:
                try:
                    my_cl.add_contact(edit)
                    print('Удачное создание контакта.')
                except RuntimeError:
                    LOGGER.error('Не удалось отправить информацию на сервер.')


def sender_message(my_cl):
    dest = input('Введите имя получателя или \'!!!\' для завершения работы: ')
    if dest == '!!!':
        return
        # Проверим, что получатель существует
    with database_lock:
        if not my_cl.database.check_user(dest):
            LOGGER.error(f'Попытка отправить сообщение незарегистрированому получателю: {dest}')
            return
    message = input('Введите сообщение для отправки или \'!!!\' для завершения работы: ')
    if message == '!!!':
        return
    my_cl.send_message(message, dest)


def listen_func(my_cl):
    # Поток приёма:
    try:
        while True:
            sender, message = my_cl.message_from_server()
            if sender is None:
                print('Получено некорректное сообщение от сервера!')
            else:
                print(f'Получено сообщение от пользователя {sender}:\n{message}')
    except (ConnectionResetError, ConnectionError, ConnectionAbortedError):
        LOGGER.error(f'Соединение с сервером {my_cl.server_address} было потеряно.')
        sys.exit(1)
