"""Программа-клиент"""
import sys
from os import path, urandom, getcwd
from PyQt5.QtWidgets import QApplication
from Crypto.PublicKey import RSA
from common.jimbase import JIMBase
from common.decorator import LOGGER
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
        client_passwd = sys.argv[4]
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
        client_passwd = ''
    except ValueError:
        LOGGER.error('В качестве порта может быть указано только число в диапазоне от 1024 до 65535.')
        print('В качестве порта может быть указано только число в диапазоне от 1024 до 65535.')
        sys.exit(1)

    # Создаём клиентское приложение
    client_app = QApplication(sys.argv)

    # Если имя пользователя не было указано в командной строке то запросим его
    if client_name == '':
        start_dialog = UserNameDialog()
        client_app.exec_()
        # Если пользователь ввёл имя и нажал ОК, то сохраняем ведённое и удаляем объект, инааче выходим
        if start_dialog.ok_pressed:
            client_name = start_dialog.client_name.text()
            client_passwd = start_dialog.client_passwd.text()
            print(f'Поздравляю! Вы под логином {client_name} и с паролем {client_passwd}!')
            del start_dialog
        else:
            sys.exit(0)

    # Загружаем ключи с файла, если же файла нет, то генерируем новую пару.
    dir_path = getcwd()
    key_file = path.join(dir_path, f'{client_name}.key')
    if not path.exists(key_file):
        keys = RSA.generate(2048, urandom)
        with open(key_file, 'wb') as key:
            key.write(keys.export_key())
    else:
        with open(key_file, 'rb') as key:
            keys = RSA.import_key(key.read())

    # Подключаемся к серверу
    my_client = JIMClient()
    my_client.keys = keys
    my_client.start(server_address, server_port, client_name, client_passwd)
    if not my_client.started:
        print(f'Не удалось установить подключение к серверу {server_address}:{server_port}')
        sys.exit(1)
    print(f'Установлено подключение с сервером {server_address}:{server_port}')

    # !!!keys.publickey().export_key()
    LOGGER.debug("Keys successfully loaded.")

    # Инициализация БД
    my_client.database_load(client_name)

    # Создаём GUI
    main_window = ClientMainWindow(my_client)
    main_window.setWindowTitle(f'Чат Программа alpha release - {client_name}')
    client_app.exec_()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOGGER.critical(f"Необработанная ошибка: {e}")
