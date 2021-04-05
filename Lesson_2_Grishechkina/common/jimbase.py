"""Константы"""
import os
from sys import path
from time import time
path.append(os.path.join(os.getcwd(), '..'))
from decorator import Log, LOGGER


class JIMBase:
    # Порт по умолчанию для сетевого ваимодействия
    DEFAULT_PORT = 7777
    # IP адрес по умолчанию для подключения клиента
    DEFAULT_IP_ADDRESS = '127.0.0.1'
    # Максимальная очередь подключений
    MAX_CONNECTIONS = 5


    # Прококол JIM основные ключи:
    ACTION = 'action'
    TIME = 'time'
    USER = 'user'
    ACCOUNT_NAME = 'account_name'
    SENDER = 'sender'
    DESTINATION = 'to'

    # Прочие ключи, используемые в протоколе
    PRESENCE = 'presence'
    RESPONSE = 'response'
    ERROR = 'error'
    MESSAGE = 'message'
    MESSAGE_TEXT = 'mess_text'

    BAD_REQUEST = {
        RESPONSE: 400,
        ERROR: 'Bad Request'
    }

    @classmethod
    @Log()
    def create_message(cls, text, from_acc_name, to_acc_name):
        """Функция запрашивает текст сообщения и возвращает его.
        Так же завершает работу при вводе подобной комманды
        """
        message_dict = {
            cls.ACTION: cls.MESSAGE,
            cls.SENDER: from_acc_name,
            cls.DESTINATION: to_acc_name,
            cls.TIME: time(),
            cls.MESSAGE_TEXT: text
        }
        LOGGER.debug(f'Сформирован словарь сообщения: {message_dict}')
        return message_dict
