"""
Класс с тестами
"""
import sys
import os
import unittest
sys.path.append(os.path.join(os.getcwd(), '..'))
from common.jimbase import JIMBase  # RESPONSE, ERROR, USER, ACCOUNT_NAME, TIME, ACTION, PRESENCE
from client import JIMClient


class TestMyFunction(unittest.TestCase):
    def test_def_presence(self):
        """Тест коректного запроса с корректным словарем"""
        test = JIMClient.create_presence()
        test[JIMBase.TIME] = 1.3  # время необходимо приравнять принудительно иначе будет ошибка
        self.assertEqual(test, {JIMBase.ACTION: JIMBase.PRESENCE,
                                JIMBase.TIME: 1.3,
                                JIMBase.USER: {JIMBase.ACCOUNT_NAME: 'Guest'}})

    def test_success_ans(self):
        """Тест корректтного разбора ответа Hello!"""
        self.assertEqual(JIMClient.process_ans({JIMBase.RESPONSE: 200}), 'Hello!')

    def test_error_ans(self):
        """Тест корректного разбора Error"""
        self.assertEqual(JIMClient.process_ans({JIMBase.RESPONSE: 400,
                                                JIMBase.ERROR: 'Bad Request'}), 'Error: Bad Request')

    def test_no_response(self):
        """Тест исключения без поля RESPONSE"""
        self.assertRaises(ValueError, JIMClient.process_ans, {JIMBase.ERROR: 'Bad Request'})


if __name__ == '__main__':
    unittest.main()
