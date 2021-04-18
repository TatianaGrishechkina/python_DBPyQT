"""Unit-тесты доп файла"""

import sys
import os
import unittest
import json
sys.path.append(os.path.join(os.getcwd(), '..'))
from common.jimbase import JIMBase
from common.json_messenger import JSONMessenger


class TestSocket:
    """
    Тестовый класс для тестирования отправки и получения,
    при создании требует словарь, который будет прогонятся
    через тестовую функцию. Тестовая заглушка, эмулирующая работу сокета.
    """
    def __init__(self, test_dict):
        self.test_dict = test_dict
        self.en_message = None
        self.r_message = None

    def send(self, message_to_send):
        """
        Тестовая функция отправки, корретно  кодирует сообщение,
        так-же сохраняет что должно было отправлено в сокет.
        message_to_send - то, что отправляем в сокет
        :param message_to_send:
        :return:
        """
        json_test_message = json.dumps(self.test_dict)
        # кодирует сообщение
        self.en_message = json_test_message.encode(JSONMessenger.ENCODING)
        # сохраняем что должно было отправлено в сокет
        self.r_message = message_to_send

    def recv(self, max_len):
        """
        Получаем данные из сокета
        :param max_len:
        :return:
        """
        json_test_message = json.dumps(self.test_dict)
        return json_test_message.encode(JSONMessenger.ENCODING)


class Tests(unittest.TestCase):
    """Тестовый класс, собственно выполняющий тестирование."""
    test_dict_send = {
        JIMBase.ACTION: JIMBase.PRESENCE,
        JIMBase.TIME: 111111.111111,
        JIMBase.USER: {
            JIMBase.ACCOUNT_NAME: 'test_test'
        }
    }
    test_dict_recv_ok = {JIMBase.RESPONSE: 200}
    test_dict_recv_err = {
        JIMBase.RESPONSE: 400,
        JIMBase.ERROR: 'Bad Request'
    }

    def test_send_message(self):
        """
        Тестируем корректность работы фукции отправки,
        создадим тестовый сокет и проверим корректность отправки словаря
        :return:
        """
        # экземпляр тестового словаря, хранит собственно тестовый словарь
        test_socket = TestSocket(self.test_dict_send)
        # вызов функции send_message, результаты будут сохранены в тестовом сокете test_socket
        JSONMessenger(test_socket).send_message(self.test_dict_send)
        # проверка корретности кодирования словаря.
        # сравниваем результат нашего кодирования и результат от тестируемой функции
        self.assertEqual(test_socket.en_message, test_socket.r_message)
        # дополнительно, проверим исключения, если на входе не словарь.
        with self.assertRaises(Exception):
            JSONMessenger(test_socket).send_message()

    def test_get_message(self):
        """
        Тест функции приёма сообщения с сокета
        :return:
        """
        test_sock_ok = TestSocket(self.test_dict_recv_ok)
        test_sock_err = TestSocket(self.test_dict_recv_err)
        # тест корректной расшифровки корректного словаря
        self.assertEqual(JSONMessenger(test_sock_ok).get_message(), self.test_dict_recv_ok)
        # тест корректной расшифровки ошибочного словаря
        self.assertEqual(JSONMessenger(test_sock_err).get_message(), self.test_dict_recv_err)


if __name__ == '__main__':
    unittest.main()
