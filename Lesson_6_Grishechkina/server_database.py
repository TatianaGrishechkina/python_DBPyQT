from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, DateTime, Text
from sqlalchemy.orm import mapper, sessionmaker
from common.jimbase import JIMBase
from decorator import LOGGER
from datetime import datetime


# Класс - серверная база данных:
class ServerDB:
    # Класс - отображение таблицы всех пользователей
    # Экземпляр этого класса = запись в таблице AllUsers
    class ChatUsers:
        def __init__(self, username, passwd_hash):
            self.name = username
            self.last_login = datetime.now()
            self.passwd_hash = passwd_hash
            self.pubkey = None
            self.id = None

    # Класс - отображение таблицы активных пользователей:
    # Экземпляр этого класса = запись в таблице ActiveUsers
    class ActiveUsers:
        def __init__(self, user_id, ip_address, port, login_time):
            self.user = user_id
            self.ip_address = ip_address
            self.port = port
            self.login_time = login_time
            self.id = None

    # Класс - отображение таблицы истории входов
    # Экземпляр этого класса = запись в таблице LoginHistory
    class LoginHistory:
        def __init__(self, name, date, ip, port):
            self.id = None
            self.name = name
            self.date_time = date
            self.ip = ip
            self.port = port

    # Класс - отображение таблицы контактов пользователей
    class UsersContacts:
        def __init__(self, user, contact):
            self.id = None
            self.user = user
            self.contact = contact

    # Класс отображение таблицы истории действий
    class UsersHistory:
        def __init__(self, user):
            self.id = None
            self.user = user
            self.sent = 0
            self.accepted = 0

    class ServerDBSession:
        def __init__(self, db):
            self.db = db
            self.session = db.session_maker()

        # Функция выполняющяяся при входе пользователя, записывает в базу факт входа
        def user_login(self, username, ip_address, port, key):
            """
            Метод выполняющийся при входе пользователя, записывает в базу факт входа
            Обновляет открытый ключ пользователя при его изменении.
            :param username:
            :param ip_address:
            :param port:
            :param key:
            :return:
            """
            # print(username, ip_address, port)
            # Запрос в таблицу пользователей на наличие там пользователя с таким именем
            check_users = self.session.query(self.db.ChatUsers).filter_by(name=username)
            # Если имя пользователя уже присутствует в таблице, обновляем время последнего входа
            if check_users.count():
                user = check_users.first()
                user.last_login = datetime.now()
                if user.pubkey != key:
                    user.pubkey = key
            # Если нету, то генерируем исключение
            else:
                raise ValueError('Пользователь не зарегистрирован.')

            # Теперь можно создать запись в таблицу активных пользователей о факте входа.
            # Создаем экземпляр класса self.ActiveUsers, через который передаем данные в таблицу
            new_active_user = self.db.ActiveUsers(user.id, ip_address, port, datetime.now())
            self.session.add(new_active_user)

            # и сохранить в историю входов
            # Создаем экземпляр класса self.LoginHistory, через который передаем данные в таблицу
            history = self.db.LoginHistory(user.id, datetime.now(), ip_address, port)
            self.session.add(history)

            # Сохраняем изменения
            self.session.commit()

        def add_user(self, name, passwd_hash):
            """
            Метод регистрации пользователя.
            Принимает имя и хэш пароля, создаёт запись в таблице статистики.
            :param name:
            :param passwd_hash:
            :return:
            """
            # Создаем экземпляр класса self.AllUsers, через который передаем данные в таблицу
            user = self.db.ChatUsers(name, passwd_hash)
            self.session.add(user)
            self.session.flush()
            user_history = self.db.UsersHistory(user.id)
            self.session.add(user_history)
            # Комит здесь нужен, чтобы присвоился ID
            self.session.commit()

        def remove_user(self, name):
            """
            Метод удаляющий пользователя из базы.
            :param name:
            :return:
            """
            user = self.session.query(self.db.ChatUsers).filter_by(name=name).first()
            self.session.query(self.db.ActiveUsers).filter_by(user=user.id).delete()
            self.session.query(self.db.LoginHistory).filter_by(name=user.id).delete()
            self.session.query(self.db.UsersContacts).filter_by(user=user.id).delete()
            self.session.query(
                self.db.UsersContacts).filter_by(
                contact=user.id).delete()
            self.session.query(self.db.UsersHistory).filter_by(user=user.id).delete()
            self.session.query(self.db.ChatUsers).filter_by(name=name).delete()
            self.session.commit()

        def get_hash(self, name):
            """
            Метод получения хэша пароля пользователя.
            :param name:
            :return:
            """
            user = self.session.query(self.db.ChatUsers).filter_by(name=name).first()
            return user.passwd_hash

        def get_pubkey(self, name):
            """
            Метод получения публичного ключа пользователя.
            :param name:
            :return:
            """
            user = self.session.query(self.db.ChatUsers).filter_by(name=name).first()
            return user.pubkey

        def check_user(self, name):
            """
            Метод проверяющий существование пользователя.
            :param name:
            :return:
            """
            if self.session.query(self.db.ChatUsers).filter_by(name=name).count():
                return True
            else:
                return False


        # Функция фиксирующая отключение пользователя
        def user_logout(self, username):
            # Запрашиваем пользователя, что покидает нас
            # получаем запись из таблицы AllUsers
            user = self.session.query(self.db.ChatUsers).filter_by(name=username).first()

            # Удаляем его из таблицы активных пользователей.
            # Удаляем запись из таблицы ActiveUsers
            self.session.query(self.db.ActiveUsers).filter_by(user=user.id).delete()

            # Применяем изменения
            self.session.commit()

        # Функция фиксирует передачу сообщения и делает соответствующие отметки в БД
        def process_message(self, sender_name, recipient_name):
            # Получаем строки отправителя и получателя
            sender = self.session.query(self.db.ChatUsers).filter_by(name=sender_name).first()
            if sender is None:
                LOGGER.error(f'process_message не нашел пользователя "{sender_name}" в таблице ChatUsers')
                return
            recipient = self.session.query(self.db.ChatUsers).filter_by(name=recipient_name).first()
            if recipient is None:
                LOGGER.error(f'process_message не нашел пользователя "{recipient_name}" в таблице ChatUsers')
                return
            # Запрашиваем строки из истории и увеличиваем счётчики
            sender_row = self.session.query(self.db.UsersHistory).filter_by(user=sender.id).first()
            sender_row.sent += 1
            recipient_row = self.session.query(self.db.UsersHistory).filter_by(user=recipient.id).first()
            recipient_row.accepted += 1

            self.session.commit()

        # Функция добавляет контакт для пользователя.
        def add_contact(self, user, contact):
            # Получаем ID пользователей
            user = self.session.query(self.db.ChatUsers).filter_by(name=user).first()
            contact = self.session.query(self.db.ChatUsers).filter_by(name=contact).first()

            # Проверяем что не дубль и что контакт может существовать (полю пользователь мы доверяем)
            if not contact or self.session.query(self.db.UsersContacts).filter_by(user=user.id,
                                                                                  contact=contact.id).count():
                return

            # Создаём объект и заносим его в базу
            contact_row = self.db.UsersContacts(user.id, contact.id)
            self.session.add(contact_row)
            self.session.commit()

        # Функция удаляет контакт из базы данных
        def remove_contact(self, user, contact):
            # Получаем ID пользователей
            user = self.session.query(self.db.ChatUsers).filter_by(name=user).first()
            contact = self.session.query(self.db.ChatUsers).filter_by(name=contact).first()

            # Проверяем что контакт может существовать (полю пользователь мы доверяем)
            if not contact:
                return

            # Удаляем требуемое
            print(self.session.query(self.db.UsersContacts).filter(
                self.db.UsersContacts.user == user.id,
                self.db.UsersContacts.contact == contact.id
            ).delete())
            self.session.commit()

        # Функция возвращает список известных пользователей со временем последнего входа (в виде кортежей).
        def users_list(self):
            return self.session.query(self.db.ChatUsers.name).all()

        # Функция возвращает список активных пользователей
        def active_users_list(self):
            # Запрашиваем соединение таблиц и собираем кортежи имя, адрес, порт, время.
            return self.session.query(
                self.db.ChatUsers.name,
                self.db.ActiveUsers.ip_address,
                self.db.ActiveUsers.port,
                self.db.ActiveUsers.login_time
            ).join(self.db.ChatUsers).all()

        # Функция возвращающая историю входов по пользователю или всем пользователям
        def login_history(self, username=None):
            # Запрашиваем историю входа
            query = self.session.query(self.db.ChatUsers.name,
                                       self.db.LoginHistory.date_time,
                                       self.db.LoginHistory.ip,
                                       self.db.LoginHistory.port
                                       ).join(self.db.ChatUsers).order_by(self.db.LoginHistory.date_time)
            # Если было указано имя пользователя, то фильтруем по нему
            if username:
                query = query.filter(self.db.ChatUsers.name == username)
            return query.all()

        # Функция возвращает список контактов пользователя.
        def get_contacts(self, username):
            # Запрашивааем указанного пользователя
            user = self.session.query(self.db.ChatUsers).filter_by(name=username).one()

            # Запрашиваем его список контактов
            query = self.session.query(self.db.UsersContacts, self.db.ChatUsers.name). \
                filter_by(user=user.id). \
                join(self.db.ChatUsers, self.db.UsersContacts.contact == self.db.ChatUsers.id)

            # выбираем только имена пользователей и возвращаем их.
            return [contact[1] for contact in query.all()]

        # Функция возвращает количество переданных и полученных сообщений
        def message_history(self):
            query = self.session.query(
                self.db.ChatUsers.name,
                self.db.UsersHistory.sent,
                self.db.UsersHistory.accepted
            ).join(self.db.ChatUsers)
            # Возвращаем список кортежей
            return query.all()

    def __init__(self, path):
        # Создаём движок базы данных
        # SERVER_DATABASE - sqlite:///server_database.db3
        # echo=False - отключаем ведение лога (вывод sql-запросов)
        # pool_recycle - По умолчанию соединение с БД через 8 часов простоя обрывается.
        # Чтобы это не случилось нужно добавить опцию pool_recycle = 7200 (переуст-ка соед-я через 2 часа)
        self.database_engine = create_engine(f'sqlite:///{path}', echo=False, pool_recycle=7200,
                                             connect_args={'check_same_thread': False})

        # Создаём объект MetaData
        self.metadata = MetaData()

        # Создаём таблицу пользователей
        chat_users_table = Table('Chat_Users', self.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('name', String, unique=True),
                                 Column('passwd_hash', String),
                                 Column('pubkey', Text)
                                 )

        # Создаём таблицу активных пользователей
        active_users_table = Table('Active_Users', self.metadata,
                                   Column('id', Integer, primary_key=True),
                                   Column('user', ForeignKey('Chat_Users.id'), unique=True),
                                   Column('ip_address', String),
                                   Column('port', Integer),
                                   Column('login_time', DateTime)
                                   )

        # Создаём таблицу истории входов
        login_history_table = Table('Login_History', self.metadata,
                                    Column('id', Integer, primary_key=True),
                                    Column('name', ForeignKey('Chat_Users.id')),
                                    Column('date_time', DateTime),
                                    Column('ip', String),
                                    Column('port', String)
                                    )

        # Создаём таблицу контактов пользователей
        users_contacts_table = Table('Users_Contacts', self.metadata,
                                     Column('id', Integer, primary_key=True),
                                     Column('user', ForeignKey('Chat_Users.id')),
                                     Column('contact', ForeignKey('Chat_Users.id'))
                                     )

        # Создаём таблицу истории пользователей
        users_history_table = Table('Users_History', self.metadata,
                                    Column('id', Integer, primary_key=True),
                                    Column('user', ForeignKey('Chat_Users.id')),
                                    Column('sent', Integer),
                                    Column('accepted', Integer)
                                    )

        # Создаём таблицы
        self.metadata.create_all(self.database_engine)

        # Создаём отображения
        # Связываем класс в ORM с таблицей
        mapper(self.ChatUsers, chat_users_table)
        mapper(self.ActiveUsers, active_users_table)
        mapper(self.LoginHistory, login_history_table)
        mapper(self.UsersContacts, users_contacts_table)
        mapper(self.UsersHistory, users_history_table)

        # Создаём сессию
        self.session_maker = sessionmaker(bind=self.database_engine)
        session = self.session_maker()

        # Если в таблице активных пользователей есть записи, то их необходимо удалить
        # Когда устанавливаем соединение, очищаем таблицу активных пользователей
        session.query(self.ActiveUsers).delete()
        session.commit()

    def create_session(self):
        return self.ServerDBSession(self)


# Отладка
if __name__ == '__main__':
    test_db = ServerDB()
    session = test_db.create_session()
    # выполняем 'подключение' пользователя и выводим список активных пользователей
    session.user_login('client_1', '192.168.1.4', 8888)
    print(session.active_users_list())
    # подключаем другого пользователя и выводим список активных пользователей
    session.user_login('client_2', '192.168.1.5', 7777)
    print(session.active_users_list())
    # выполянем 'отключение' пользователя
    session.user_logout('client_1')
    # выводим список активных пользователей
    print(session.active_users_list())
    # пользователь опять заходит и выходит
    session.user_login('client_1', '192.168.1.4', 8888)
    session.user_logout('client_1')
    # запрашиваем историю входов по пользователю
    print(session.login_history('client_1'))
    # выводим список известных пользователей
    print(session.users_list())
    session.add_contact('test2', 'test1')
    session.add_contact('test1', 'test3')
    session.add_contact('test1', 'test6')
    session.remove_contact('test1', 'test3')
    session.process_message('client_1', 'client_2')
    print(session.message_history())
