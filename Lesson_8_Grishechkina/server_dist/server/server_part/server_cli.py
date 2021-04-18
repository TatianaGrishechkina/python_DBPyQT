import time
from threading import Thread


def show_help():
    print("""
            Поддерживаемые комманды:
            help - это меню
            users - общий список пользователей
            conn - пользователи онлайн
            lh - история входов пользователя
            exit - завершение работы сервера
    """)


def interface_func(database):
    session = database.create_session()
    show_help()
    while True:
        command = input('Введите комманду: ')
        if command == 'help':
            show_help()
        elif command == 'exit':
            break
        elif command == 'users':
            for user in sorted(session.users_list()):
                print({user[0]})
        elif command == 'conn':
            for user in sorted(session.active_users_list()):
                print(f'Пользователь {user[0]}, подключен: {user[1]}:{user[2]}, время установки соединения: {user[3]}')
        elif command == 'lh':
            name = input('Введите имя конкретного пользователя. Для вывода всей истории, просто нажмите Enter: ')
            for user in session.login_history(name):
                print(f'Пользователь: {user[0]} время входа: {user[1]}. Вход с: {user[2]}:{user[3]}')
        else:
            print('Команда не распознана.')


def run_server_cli(server_thread, database):
    # интерфейс - другим
    interface_thread = Thread(target=interface_func, args=(database,))
    interface_thread.daemon = True
    interface_thread.start()

    while True:
        time.sleep(1)
        if server_thread.is_alive() and interface_thread.is_alive():
            continue
        break
