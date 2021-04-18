Client module
==============

Клиентский модуль мессенджера. Служит для обмена сообщений с другими клиентами через сервер.

Использование:

Модулю нужны параметры, такие как IP адрес сервера для подключения, порт, имя клиента и пароль.

Пример запуска:

``python client.py 127.0.0.1 7777 KIBORG1 123456``

client.py
~~~~~~~~~

Содержит только функцию main(), которая:

* загружает файл конфигурации с командной строки,
* создает клиентское приложение,
* запрашивает имя пользователя и пароль, если они не были указаны в командной строке,
* загружает ключи с файла либо генерирует новую пару,
* подключается к серверу (класс JIMClient(), путь - client_part.transport.py,
* инициализирует базу данных (метод database_load класса JIMClient()),
* запускает пользовательский интерфейс через класс ClientMainWindow (client_part.main_window.py)

main_window.py
~~~~~~~~~~~~~~
.. autoclass:: client_part.main_window.ClientMainWindow
    :members:

add_contact.py
~~~~~~~~~~~~~~
.. autoclass:: client_part.add_contact.AddContactDialog
    :members:

del_contact.py
~~~~~~~~~~~~~~
.. autoclass:: client_part.del_contact.DelContactDialog
    :members:

start_dialog.py
~~~~~~~~~~~~~~~
.. autoclass:: client_part.start_dialog.UserNameDialog
    :members:

transport.py
~~~~~~~~~~~~
.. autoclass:: client_part.transport.JIMClient
    :members:

client_database.py
~~~~~~~~~~~~~~~~~~
.. autoclass:: client_part.client_database.ClientDB
    :members:

