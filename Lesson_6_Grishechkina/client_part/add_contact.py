import sys
import logging
from PyQt5.QtWidgets import QDialog, QLabel, QComboBox, QPushButton
from PyQt5.QtCore import Qt
sys.path.append('../')

logger = logging.getLogger('client')


# Диалог выбора контакта для добавления
class AddContactDialog(QDialog):
    def __init__(self, transport):
        super().__init__()
        self.transport = transport
        self.db_session = transport.database.create_session()

        self.setFixedSize(350, 120)
        self.setWindowTitle('Выберите контакт для добавления:')
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setModal(True)

        self.selector_label = QLabel('Выберите контакт для добавления:', self)
        self.selector_label.setFixedSize(200, 20)
        self.selector_label.move(10, 0)

        self.selector = QComboBox(self)
        self.selector.setFixedSize(200, 20)
        self.selector.move(10, 30)

        self.btn_refresh = QPushButton('Обновить список', self)
        self.btn_refresh.setFixedSize(100, 30)
        self.btn_refresh.move(60, 60)

        self.btn_ok = QPushButton('Добавить', self)
        self.btn_ok.setFixedSize(100, 30)
        self.btn_ok.move(230, 20)

        self.btn_cancel = QPushButton('Отмена', self)
        self.btn_cancel.setFixedSize(100, 30)
        self.btn_cancel.move(230, 60)
        self.btn_cancel.clicked.connect(self.close)

        # Заполняем список возможных контактов
        self.possible_contacts_update()
        # Назначаем действие на кнопку обновить
        self.btn_refresh.clicked.connect(self.update_possible_contacts)

    # Заполняем список возможных контактов разницей между всеми пользователями и
    def possible_contacts_update(self):
        print('possible_contacts_update')
        self.selector.clear()
        # множества всех контактов и контактов клиента
        contacts_list = set(self.db_session.get_contacts())
        users_list = set(self.db_session.get_users())
        # Удалим сами себя из списка пользователей, чтобы нельзя было добавить самого себя
        users_list.remove(self.transport.client_name)  # (self.transport.username)
        # Добавляем список возможных контактов
        self.selector.addItems(users_list - contacts_list)

    # Обновлялка возможных контактов. Обновляет таблицу известных пользователей,
    # затем содержимое предполагаемых контактов
    def update_possible_contacts(self):
        try:
            print('update_possible_contacts')
            self.transport.user_list_request(self.transport.client_name)
        except OSError:
            pass
        else:
            logger.debug('Обновление списка пользователей с сервера выполнено')
            self.possible_contacts_update()
