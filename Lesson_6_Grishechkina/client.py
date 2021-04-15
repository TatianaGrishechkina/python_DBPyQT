"""Программа-клиент"""
import sys
# import json
# import socket
import time
# from errors import ReqFieldMissingError, NonDictInputError
from PyQt5.QtWidgets import QApplication
from common.jimbase import JIMBase
from decorator import LOGGER
from threading import Thread
from client_part.transport import JIMClient
from client_part.start_dialog import UserNameDialog
from client_part.main_window import ClientMainWindow


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

    # Создаём клиентокое приложение
    client_app = QApplication(sys.argv)

    # Если имя пользователя не было указано в командной строке то запросим его
    if client_name == '':
        start_dialog = UserNameDialog()
        client_app.exec_()
        # Если пользователь ввёл имя и нажал ОК, то сохраняем ведённое и удаляем объект, инааче выходим
        if start_dialog.ok_pressed:
            client_name = start_dialog.client_name.text()
            print(f'Поздравляю! Вы под логином {client_name}!')
            del start_dialog
        else:
            exit(0)

    my_client = JIMClient()
    my_client.start(server_address, server_port, client_name)
    print(f'Установлено подключение с сервером {server_address}:{server_port}')

    # Инициализация БД
    my_client.database_load(client_name)

    # затем запускаем отправку сообщений и взаимодействие с пользователем через терминал (CLI)
    '''
    user_interface = Thread(target=sender_func, args=(my_client,))
    user_interface.daemon = True
    user_interface.start()
    LOGGER.debug('Запущены процессы')'''

    # Создаём GUI
    main_window = ClientMainWindow(my_client)
    main_window.setWindowTitle(f'Чат Программа alpha release - {client_name}')
    client_app.exec_()

    # Watchdog основной цикл, если один из потоков завершён,
    # то значит или потеряно соединение или пользователь
    # ввёл exit. Поскольку все события обработываются в потоках,
    # достаточно просто завершить цикл.
    # Для GUI не нужно
    '''while True:
        time.sleep(1)
        if receiver.is_alive() and user_interface.is_alive():
            continue
        break'''


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOGGER.critical(f"Необработанная ошибка: {e}")
