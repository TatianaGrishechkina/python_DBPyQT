"""Кофнфиг серверного логгера"""

import sys
import os
import logging
import logging.handlers
# sys.path.append('../')

# Текущий уровень логирования
LOGGING_LEVEL = logging.DEBUG

# создаём формировщик логов (formatter):
SERVER_FORMATTER = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(message)s')

# Подготовка имени файла для логирования
PATH = os.getcwd()
PATH = os.path.join(PATH, 'server.log')

# создаём потоки вывода логов
STREAM_HANDLER = logging.StreamHandler(sys.stderr)
STREAM_HANDLER.setFormatter(SERVER_FORMATTER)
# устанавливаем для потока вывода уровень ERROR
STREAM_HANDLER.setLevel(logging.ERROR)
# файл формируем с ротацией - раз в час
LOG_FILE = logging.handlers.TimedRotatingFileHandler(PATH, encoding='utf8', interval=1, when='D')
LOG_FILE.setFormatter(SERVER_FORMATTER)

# создаём регистратор и настраиваем его
LOGGER = logging.getLogger('server')
LOGGER.addHandler(STREAM_HANDLER)
LOGGER.addHandler(LOG_FILE)
LOGGER.setLevel(LOGGING_LEVEL)

# отладка
if __name__ == '__main__':
    LOGGER.critical('Критическая ошибочка')
    LOGGER.error('Ошибочка')
    LOGGER.debug('Отладочная информация')
    LOGGER.info('Информационное сообщение')
