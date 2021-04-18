"""Утилиты"""
import json
import sys
import os
sys.path.append(os.path.join(os.getcwd(), '..'))
from common.errors import NonDictInputError
from common.decorator import Log


class JSONMessenger:
    """
    Класс для приема и отправки сообщений между клиентом и сервером.
    """
    # Максимальная длинна сообщения в байтах
    MAX_PACKAGE_LENGTH = 1024
    # Кодировка проекта
    ENCODING = 'utf-8'

    def __init__(self, sock):
        self.sock = sock

    @Log()
    def get_message(self):
        """
        Метод приёма и декодирования сообщения
        принимает байты, выдаёт словарь, а если принято что-то другое -- отдаёт ошибку значения.
        :return: возвращает словарь, полученный по сокету
        """
        encoded_response = self.sock.recv(self.MAX_PACKAGE_LENGTH)
        if not isinstance(encoded_response, bytes):
            raise ValueError
        json_response = encoded_response.decode(self.ENCODING)
        response = json.loads(json_response)
        if not isinstance(response, dict):
            raise NonDictInputError
        return response

    @Log()
    def send_message(self, message):
        """
        Метод кодирования и отправки сообщения принимает словарь и отправляет его.
        :param message: сообщение в виде словаря
        """

        js_message = json.dumps(message)
        encoded_message = js_message.encode(self.ENCODING)
        self.sock.send(encoded_message)
