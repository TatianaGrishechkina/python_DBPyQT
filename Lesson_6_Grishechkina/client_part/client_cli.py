import sys
from decorator import Log, LOGGER
sys.path.append('../')


class ClientCLI:
    def __init(self, transport):
        self.transport = transport
        self.db_session = transport.database.create_session()

    # Поток отправки и взаимодействия с пользователем
    def sender_func(self):
        try:
            while True:
                self.print_help()
                while True:
                    command = input('Введите команду: ')
                    # Если отправка сообщения - соответствующий метод
                    if command == 'message':
                        self.sender_message(my_cl)

                    # Вывод помощи
                    elif command == 'help':
                        self.print_help()

                    # Выход. Отправляем сообщение серверу о выходе.
                    elif command == 'exit':
                        try:
                            self.transport.stop()
                        except:
                            pass
                        print('Спасибо за использование нашего сервиса!')
                        LOGGER.info('Завершение работы по команде пользователя.')
                        # Задержка неоходима, чтобы успело уйти сообщение о выходе
                        time.sleep(0.5)
                        sys.exit(0)

                    # Список контактов
                    elif command == 'contacts':
                        contacts_list = self.db_session.get_contacts()
                        for contact in contacts_list:
                            print(contact)

                    # Редактирование контактов
                    elif command == 'edit':
                        self.edit_contacts()

                    # история сообщений.
                    elif command == 'history':
                        self.print_history()

                    else:
                        print('Команда не распознана, попробойте снова. help - вывести поддерживаемые команды.')

        except (ConnectionResetError, ConnectionError, ConnectionAbortedError):
            LOGGER.error(f'Соединение с сервером {self.transport.server_address} было потеряно.')
            sys.exit(1)

    # Функция выводящяя справку по использованию.
    def print_help(self):
        print('Поддерживаемые команды:')
        print('message - отправить сообщение. Кому и текст будет запрошены отдельно.')
        print('history - история сообщений')
        print('contacts - список контактов')
        print('edit - редактирование списка контактов')
        print('help - вывести подсказки по командам')
        print('exit - выход из программы')

    # Функция выводящяя историю сообщений
    def print_history(self):
        ask = input('Показать входящие сообщения - in, исходящие - out, все - просто Enter: ')
        #with database_lock:
        if ask == 'in':
            history_list = self.db_session.get_history(to_who=self.transport.client_name)
            for message in history_list:
                print(f'\nСообщение от пользователя: {message[0]} от {message[3]}:\n{message[2]}')
        elif ask == 'out':
            history_list = self.db_session.get_history(from_who=self.transport.client_name)
            for message in history_list:
                print(f'\nСообщение пользователю: {message[1]} от {message[3]}:\n{message[2]}')
        else:
            history_list = self.db_session.get_history()
            for message in history_list:
                print(f'\nСообщение от пользователя: {message[0]}, пользователю {message[1]} от {message[3]}\n{message[2]}')

    # Функция изменеия контактов
    def edit_contacts(self):
        ans = input('Для удаления введите del, для добавления add: ')
        if ans == 'del':
            edit = input('Введите имя удаляемного контакта: ')
            if self.db_session.check_contact(edit):
                self.db_session.del_contact(edit)
            else:
                LOGGER.error('Попытка удаления несуществующего контакта.')
        elif ans == 'add':
            # Проверка на возможность такого контакта
            edit = input('Введите имя создаваемого контакта: ')
            if self.db_session.check_user(edit):
                self.db_session.add_contact(edit)
                with self.transport.lock:
                    try:
                        self.transport.add_contact(edit)
                        print('Удачное создание контакта.')
                    except RuntimeError:
                        LOGGER.error('Не удалось отправить информацию на сервер.')

    def sender_message(self):
        dest = input('Введите имя получателя или \'!!!\' для завершения работы: ')
        if dest == '!!!':
            return
            # Проверим, что получатель существует
        if not self.db_session.check_user(dest):
            LOGGER.error(f'Попытка отправить сообщение незарегистрированому получателю: {dest}')
            return
        message = input('Введите сообщение для отправки или \'!!!\' для завершения работы: ')
        if message == '!!!':
            return
        self.transport.send_message(message, dest)