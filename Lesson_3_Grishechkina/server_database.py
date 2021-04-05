from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, DateTime
from sqlalchemy.orm import mapper, sessionmaker
from common.jimbase import JIMBase
from datetime import datetime


# Класс - серверная база данных:
class ServerDB:
    # Класс - отображение таблицы всех пользователей
    # Экземпляр этого класса = запись в таблице AllUsers
    class ChatUsers:
        def __init__(self, username):
            self.name = username
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

    class ServerDBSession:
        def __init__(self, db):
            self.db = db
            self.session = db.session_maker()

        # Функция выполняющяяся при входе пользователя, записывает в базу факт входа
        def user_login(self, username, ip_address, port):
            # print(username, ip_address, port)
            # Запрос в таблицу пользователей на наличие там пользователя с таким именем
            check_users = self.session.query(self.db.ChatUsers).filter_by(name=username)
            # Если имя пользователя уже присутствует в таблице, обновляем время последнего входа
            if check_users.count():
                user = check_users.first()
                user.last_login = datetime.now()
            # Если нет, то создаздаём нового пользователя
            else:
                # Создаем экземпляр класса self.AllUsers, через который передаем данные в таблицу
                user = self.db.ChatUsers(username)
                self.session.add(user)
                # Комит здесь нужен, чтобы присвоился ID
                self.session.commit()

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

    def __init__(self):
        # Создаём движок базы данных
        # SERVER_DATABASE - sqlite:///server_database.db3
        # echo=False - отключаем ведение лога (вывод sql-запросов)
        # pool_recycle - По умолчанию соединение с БД через 8 часов простоя обрывается.
        # Чтобы это не случилось нужно добавить опцию pool_recycle = 7200 (переуст-ка соед-я через 2 часа)
        self.database_engine = create_engine(JIMBase.SERVER_DATABASE, echo=False, pool_recycle=7200)

        # Создаём объект MetaData
        self.metadata = MetaData()

        # Создаём таблицу пользователей
        chat_users_table = Table('Chat_Users', self.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('name', String, unique=True)
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

        # Создаём таблицы
        self.metadata.create_all(self.database_engine)

        # Создаём отображения
        # Связываем класс в ORM с таблицей
        mapper(self.ChatUsers, chat_users_table)
        mapper(self.ActiveUsers, active_users_table)
        mapper(self.LoginHistory, login_history_table)

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
