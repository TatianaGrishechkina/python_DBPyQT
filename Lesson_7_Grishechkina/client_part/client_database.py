"""Класс - база данных клиента."""
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, DateTime, or_
from sqlalchemy.orm import mapper, sessionmaker


class ClientDB:
    class KnownUsers:
        """Подкласс - отображение таблицы известных пользователей."""
        def __init__(self, user):
            self.id = None
            self.username = user

    class MessageHistory:
        """Подкласс - отображение таблицы истории сообщений."""
        def __init__(self, from_user, to_user, message):
            self.id = None
            self.from_user = from_user
            self.to_user = to_user
            self.message = message
            self.date = datetime.now()

    class Contacts:
        """Подкласс - отображение списка контактов."""
        def __init__(self, contact):
            self.id = None
            self.name = contact

    # Конструктор класса:
    def __init__(self, name):
        # Создаём движок базы данных, поскольку разрешено несколько клиентов одновременно, каждый должен иметь свою БД
        # Поскольку клиент мультипоточный необходимо отключить проверки на подключения с разных потоков,
        # иначе sqlite3.ProgrammingError
        self.database_engine = create_engine(f'sqlite:///client_{name}.db3', echo=False, pool_recycle=7200,
                                             connect_args={'check_same_thread': False})

        # Создаём объект MetaData
        self.metadata = MetaData()

        # Создаём таблицу известных пользователей
        known_users_table = Table('known_users', self.metadata,
                                  Column('id', Integer, primary_key=True),
                                  Column('username', String)
                                  )

        # Создаём таблицу истории сообщений
        message_history_table = Table('message_history', self.metadata,
                                      Column('id', Integer, primary_key=True),
                                      Column('from_user', String),
                                      Column('to_user', String),
                                      Column('message', Text),
                                      Column('date', DateTime)
                                      )

        # Создаём таблицу контактов
        contacts_table = Table('contacts', self.metadata,
                               Column('id', Integer, primary_key=True),
                               Column('name', String, unique=True)
                               )

        # Создаём таблицы
        self.metadata.create_all(self.database_engine)

        # Создаём отображения
        mapper(self.KnownUsers, known_users_table)
        mapper(self.MessageHistory, message_history_table)
        mapper(self.Contacts, contacts_table)

        # Создаём генератор сессий
        self.session_maker = sessionmaker(bind=self.database_engine)

        # Создаём сессию
        with self.session_maker() as session:
            # Необходимо очистить таблицу контактов, т.к. при запуске они подгружаются с сервера.
            session.query(self.Contacts).delete()
            session.commit()

    class ClientDBSession:
        """Подкласс - создание клиентской сессии."""
        def __init__(self, db):
            self.db = db
            self.session = db.session_maker()

        def add_contact(self, contact):
            """Метод добавления контактов."""
            if not self.session.query(self.db.Contacts).filter_by(name=contact).count():
                contact_row = self.db.Contacts(contact)
                self.session.add(contact_row)
                self.session.commit()

        def del_contact(self, contact):
            """Метод удаления контакта."""
            self.session.query(self.db.Contacts).filter_by(name=contact).delete()

        def add_users(self, users_list):
            """Метод добавления известных пользователей.
               Пользователи получаются только с сервера, поэтому таблица очищается."""
            self.session.query(self.db.KnownUsers).delete()
            for user in users_list:
                user_row = self.db.KnownUsers(user)
                self.session.add(user_row)
            self.session.commit()

        def save_message(self, from_user, to_user, message):
            """Метод сохраняющий сообщения."""
            message_row = self.db.MessageHistory(from_user, to_user, message)
            self.session.add(message_row)
            self.session.commit()

        def get_contacts(self):
            """Метод возвращающий контакты."""
            return [contact[0] for contact in self.session.query(self.db.Contacts.name).all()]

        def get_users(self):
            """Метод возвращающий список известных пользователей."""
            print('get_users')
            return [user[0] for user in self.session.query(self.db.KnownUsers.username).all()]

        def check_user(self, user):
            """Метод проверяющий наличие пользователя в известных."""
            if self.session.query(self.db.KnownUsers).filter_by(username=user).count():
                return True
            else:
                return False

        def check_contact(self, contact):
            """Метод проверяющий наличие пользователя контактах."""
            if self.session.query(self.db.Contacts).filter_by(name=contact).count():
                return True
            else:
                return False

        def get_history(self, from_who=None, to_who=None, with_who=None):
            """Метод возвращающий историю переписки."""
            query = self.session.query(self.db.MessageHistory)
            if from_who:
                query = query.filter_by(from_user=from_who)
            if to_who:
                query = query.filter_by(to_user=to_who)
            if with_who:
                query = query.filter(or_(self.db.MessageHistory.from_user == with_who,
                                         self.db.MessageHistory.to_user == with_who))
            return [(history_row.from_user, history_row.to_user, history_row.message, history_row.date)
                    for history_row in query.all()]

    # Функция инициирует клиентскую DB сессию
    def create_session(self):
        return self.ClientDBSession(self)


# отладка
if __name__ == '__main__':
    test_db = ClientDB('test1').create_session()
    for i in ['test3', 'test4', 'test5']:
        test_db.add_contact(i)
    test_db.add_contact('test4')
    test_db.add_users(['test1', 'test2', 'test3', 'test4', 'test5'])
    test_db.save_message('test1', 'test2', f'Привет! я тестовое сообщение 1 {datetime.now()}!')
    test_db.save_message('test2', 'test1', f'Привет! я тестовое сообщение 2 от {datetime.now()}!')
    print(test_db.get_contacts())
    print(test_db.get_users())
    print(test_db.check_user('test1'))
    print(test_db.check_user('test10'))
    print(test_db.get_history('test2'))
    print(test_db.get_history(to_who='test2'))
    print(test_db.get_history('test3'))
    test_db.del_contact('test4')
    print(test_db.get_contacts())
