"""Unit-тесты сервера"""

import sys
import os
import unittest
sys.path.append(os.path.join(os.getcwd(), '..'))
from common.jimbase import JIMBase
from server import JIMServer


class TestServer(unittest.TestCase):
    """В сервере только 1 функция для тестирования"""
    err_dict = {
        JIMBase.RESPONSE: 400,
        JIMBase.ERROR: 'Bad Request'
    }
    ok_dict = {JIMBase.RESPONSE: 200}

    def test_no_action(self):
        """Ошибка если нет ACTION"""
        self.assertEqual(JIMServer.process_client_message(
            {JIMBase.TIME: '1.3',
             JIMBase.USER: {JIMBase.ACCOUNT_NAME: 'Guest'}}), self.err_dict)

    def test_wrong_action(self):
        """Ошибка если ACTION неизвестен"""
        self.assertEqual(JIMServer.process_client_message(
            {JIMBase.ACTION: 'Wrong',
             JIMBase.TIME: '1.3',
             JIMBase.USER: {JIMBase.ACCOUNT_NAME: 'Guest'}}), self.err_dict)

    def test_no_time(self):
        """Ошибка, если  запрос не содержит TIME"""
        self.assertEqual(JIMServer.process_client_message(
            {JIMBase.ACTION: JIMBase.PRESENCE,
             JIMBase.USER: {JIMBase.ACCOUNT_NAME: 'Guest'}}), self.err_dict)

    def test_no_user(self):
        """Ошибка - нет ACCOUNT_NAME"""
        self.assertEqual(JIMServer.process_client_message(
            {JIMBase.ACTION: JIMBase.PRESENCE,
             JIMBase.TIME: '1.1'}), self.err_dict)

    def test_unknown_user(self):
        """Ошибка - не Guest"""
        self.assertEqual(JIMServer.process_client_message(
            {JIMBase.ACTION: JIMBase.PRESENCE,
             JIMBase.TIME: 1.1,
             JIMBase.USER: {JIMBase.ACCOUNT_NAME: 'Guest1'}}), self.err_dict)

    def test_ok_dict(self):
        """Корректный запрос"""
        self.assertEqual(JIMServer.process_client_message(
            {JIMBase.ACTION: JIMBase.PRESENCE,
             JIMBase.TIME: 1.1,
             JIMBase.USER: {JIMBase.ACCOUNT_NAME: 'Guest'}}), self.ok_dict)


if __name__ == '__main__':
    unittest.main()
