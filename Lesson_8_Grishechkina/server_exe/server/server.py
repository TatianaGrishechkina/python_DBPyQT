"""Программа-сервер"""
import os
import configparser
import sqlite3
from threading import Thread
from common.decorator import Log, LOGGER
from server_part.server_database import ServerDB
from server_part.server_config import ServerConfig
from server_part.server_gui import run_server_gui
from server_part.main_class import JIMServer, server_func


def main():
    # Загрузка файла конфигурации сервера
    ini = configparser.ConfigParser()
    dir_path = os.getcwd()
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
    # run_server_cli(server_thread, database)
    run_server_gui(my_server, config)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOGGER.critical(str(e))

