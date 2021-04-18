Server module
==============

Серверный модуль мессенджера. Обрабатывает словари -- сообщения от клиентов. Также хранит публичные ключи клиентов.

Использование:

Модулю не нужны параметры, по умолчанию запускает сервер на 127.0.0.1:7777, берет данные с server.ini файла,
который можно конфигурировать с программы.

Пример запуска:

``python server.py``

server.py
~~~~~~~~~~~~~~

Содержит только функцию main(), которая:

* загружает файл конфигурации сервера server.ini,
* читает его при помощи класса ServerConfig() (путь - server_part.server_config.py),
* инициализирует базу данных (класс ServerDB(), путь - server_part.server_database.py),
* запускает сервер при помощи функции start (класс JIMServer() server_part.main_class.py),
* запускает поток обработки сообщений сервером,
* запускает пользовательский интерфейс через функцию run_server_gui (server_part.server_gui.py)

main_class.py
~~~~~~~~~~~~~~
.. autoclass:: server_part.main_class.JIMServer
    :members:

add_user.py
~~~~~~~~~~~~~~
.. autoclass:: server_part.add_user.RegisterUser
    :members:

remove_user.py
~~~~~~~~~~~~~~
.. autoclass:: server_part.remove_user.DelUserDialog
    :members:

server_config.py
~~~~~~~~~~~~~~~~
.. automodule:: server_part.server_config
    :members:

server_database.py
~~~~~~~~~~~~~~~~~~
.. autoclass:: server_part.server_database.ServerDB
    :members:

server_gui.py
~~~~~~~~~~~~~~
.. automodule:: server_part.server_gui
    :members:

