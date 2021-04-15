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
        clients_count = int(input('Введите количество тестовых клиентов для запуска: '))
        # Запускаем клиентов:
        for i in range(clients_count):
            client_name = f'KIBORG{i + 1}'
            err_file = open(f"client_{client_name}.err", "a")
            process.append(subprocess.Popen(f'python client.py 127.0.0.1 7777 {client_name}',
                                            creationflags=subprocess.CREATE_NEW_CONSOLE))
                                            #stderr=subprocess.STDOUT))
    elif action == 'x':
        while process:
            process.pop().kill()
        while err_files:
            err_file = err_files.pop()
            err_file.flush()
            err_file.close()
