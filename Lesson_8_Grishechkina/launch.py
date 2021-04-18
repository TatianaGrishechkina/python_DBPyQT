"""Лаунчер"""
import subprocess

process = []
err_files = []

while True:
    action = input('Выберите действие: q - выход, '
                   's - запустить сервер, k - запустить клиенты, x - закрыть все окна: ')

    if action == 'q':
        break
    elif action == 's':
        process.append(subprocess.Popen('python server.py',
                                        creationflags=subprocess.CREATE_NEW_CONSOLE))
    elif action == 'k':
        print('Убедитесь, что на сервере зарегистрировано необходимо количество клиентов с паролем 123456.')
        print('Первый запуск может быть достаточно долгим из-за генерации ключей!')
        clients_count = int(input('Введите количество тестовых клиентов для запуска: '))
        # Запускаем клиентов:
        for i in range(clients_count):
            client_name = f'KIBORG{i + 1}'
            err_file = open(f"client_{client_name}.err", "a")
            process.append(subprocess.Popen(f'cmd /k python client.py 127.0.0.1 7777 {client_name} 123456',
                                            creationflags=subprocess.CREATE_NEW_CONSOLE))
                                            #stderr=subprocess.STDOUT))
    elif action == 'x':
        while process:
            process.pop().kill()
        while err_files:
            err_file = err_files.pop()
            err_file.flush()
            err_file.close()
