"""Программа-клиент"""

import sys
import json
import socket
import time
from errors import ReqFieldMissingError, NonDictInputError
from common.jimbase import JIMBase
from common.json_messenger import JSONMessenger
from decorator import Log, LOGGER
from threading import Thread
from metaclasses import ClientInspector


class JIMClient(JIMBase, metaclass=ClientInspector):
    transport = None
    messenger = None
    server_address = ''
    client_name = ''

    @Log()
    def message_from_server(self):
        """Функция - обработчик сообщений других пользователей, поступающих с сервера"""
        message = self.messenger.get_message()
        if self.ACTION in message and message[self.ACTION] == self.MESSAGE and \
                self.SENDER in message and self.MESSAGE_TEXT in message:
            LOGGER.info(f'Получено сообщение от пользователя '
                        f'{message[self.SENDER]}:\n{message[self.MESSAGE_TEXT]}')
            return message[self.SENDER], message[self.MESSAGE_TEXT]
        else:
            LOGGER.error(f'Получено некорректное сообщение с сервера: {message}')
            return None, None

    @Log()
    def send_message(self, text, dest):
        self.messenger.send_message(self.create_message(text, self.client_name, dest))

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
        except TimeoutError:
            LOGGER.error('Попытка установить соединение была безуспешной, '
                         'т.к. от другого компьютера за требуемое время не получен нужный отклик.')
            print('Попытка установить соединение была безуспешной, '
                  'т.к. от другого компьютера за требуемое время не получен нужный отклик.')

    @Log()
    def stop(self):
        self.transport.close()
        LOGGER.info('Завершение работы по команде пользователя.')


def sender_func(my_cl):
    # Поток отправки и взаимодействия с пользователем
    try:
        while True:
            dest = input('Введите имя получателя или \'!!!\' для завершения работы: ')
            if dest == '!!!':
                break
            message = input('Введите сообщение для отправки или \'!!!\' для завершения работы: ')
            if message == '!!!':
                break
            my_cl.send_message(message, dest)
        my_cl.stop()
        print('Спасибо за использование нашего сервиса!')
        sys.exit(0)
    except (ConnectionResetError, ConnectionError, ConnectionAbortedError):
        LOGGER.error(f'Соединение с сервером {my_cl.server_address} было потеряно.')
        sys.exit(1)


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


def main():
    """Загружаем параметы коммандной строки"""
    # client.py 127.0.0.1 7777 test1
    try:
        server_address = sys.argv[1]
        server_port = int(sys.argv[2])
        client_name = sys.argv[3]
        if server_port < 1024 or server_port > 65535:
            LOGGER.critical(
                f'Попытка запуска клиента с неподходящим номером порта: {server_port}.'
                f' Допустимы адреса с 1024 до 65535. Клиент завершается.')
            raise ValueError
        LOGGER.info(f'Запущен клиент с парамертами: '
                    f'адрес сервера: {server_address}, порт: {server_port}')
    except IndexError:
        server_address = JIMBase.DEFAULT_IP_ADDRESS
        server_port = JIMBase.DEFAULT_PORT
        client_name = ''
    except ValueError:
        LOGGER.error('В качестве порта может быть указано только число в диапазоне от 1024 до 65535.')
        print('В качестве порта может быть указано только число в диапазоне от 1024 до 65535.')
        sys.exit(1)

    if client_name == '':
        client_name = input('Введите имя:  ')

    print(f'Поздравляю! Вы под логином {client_name}!')

    my_client = JIMClient()
    my_client.start(server_address, server_port, client_name)
    print(f'Установлено подключение с сервером {server_address}:{server_port}')

# Если соединение с сервером установлено корректно,
    # запускаем клиенский процесс приёма сообщний
    receiver = Thread(target=listen_func, args=(my_client,))
    receiver.daemon = True
    receiver.start()

    # затем запускаем отправку сообщений и взаимодействие с пользователем.
    user_interface = Thread(target=sender_func, args=(my_client,))
    user_interface.daemon = True
    user_interface.start()
    LOGGER.debug('Запущены процессы')

    # Watchdog основной цикл, если один из потоков завершён,
    # то значит или потеряно соединение или пользователь
    # ввёл exit. Поскольку все события обработываются в потоках,
    # достаточно просто завершить цикл.
    while True:
        time.sleep(1)
        if receiver.is_alive() and user_interface.is_alive():
            continue
        break


if __name__ == '__main__':
    main()
