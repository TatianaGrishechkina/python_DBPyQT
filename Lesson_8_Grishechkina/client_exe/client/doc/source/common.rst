Common module
==============

Модуль общих для клиента и сервера утилит.
Содержит декоратор, дескрипторы, определение ошибок, переменные, классы по отправке сообщений, метаклассыю

Использование:

Утилиты вызываются при необходимости с сервера или с клиента.

decorator.py
~~~~~~~~~~~~~~
.. autoclass:: common.decorator.Log
    :members:

descriptors.py
~~~~~~~~~~~~~~
.. automodule:: common.descriptors
    :members:

errors.py
~~~~~~~~~~~~~~
.. automodule:: common.errors
    :members:

jimbase.py
~~~~~~~~~~~~~~
.. autoclass:: common.jimbase.JIMBase
    :members:

json_messenger.py
~~~~~~~~~~~~~~~~~
.. autoclass:: common.json_messenger.JSONMessenger
    :members:

metaclasses.py
~~~~~~~~~~~~~~
.. automodule:: common.metaclasses
    :members:

