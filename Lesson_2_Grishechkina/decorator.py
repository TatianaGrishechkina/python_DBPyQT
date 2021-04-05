"""Декораторы"""

import sys
import logging
import logs.config_server_log
import logs.config_client_log
import traceback
import inspect

# метод определения модуля, источника запуска.
# Метод find () возвращает индекс первого вхождения искомой подстроки,
# если он найден в данной строке.
# Если его не найдено, - возвращает -1.
# os.path.split(sys.argv[0])[1]
if sys.argv[0].find('client') > -1:
    # если не клиент то сервер!
    LOGGER = logging.getLogger('client')
else:
    # ну, раз не сервер, то клиент
    LOGGER = logging.getLogger('server')


# Реализация в виде класса
class Log:
    """Класс-декоратор"""
    def __call__(self, func):
        def log_saver(*args, **kwargs):
            """Обертка"""
            LOGGER.debug(f'Была вызвана функция {func.__name__} c параметрами {args}, {kwargs}. '
                         f'Используемый модуль: {func.__module__} и '
                         f'функция {traceback.format_stack()[0].strip().split()[-1]}. '
                         f'Также была вызвана функция {inspect.stack()[1][3]}.'
                         )
            note = func(*args, **kwargs)
            return note
        return log_saver