from dis import get_instructions


class ServerInspector(type):
    """
    Метакласс для проверки соответствия сервера.
    """
    def __init__(cls, clssname, parents, attributes):
        # clssname - экземпляр метакласса - JIMServer
        # parents - кортеж базовых классов - ()
        # attributes - словарь атрибутов и методов экземпляра метакласса

        # Список методов, используемых в функциях класса:
        methods = []
        # Атрибуты, которые используются в функциях классов
        attrs = []
        # перебираем атрибуты
        for func in attributes:
            try:
                # смотрим что это - функция или нет
                ret_attr = get_instructions(attributes[func])
                # Если не функция то ловим исключение
            except TypeError:
                pass
            else:
                # Раз функция разбираем код, получая ее методы и атрибуты.
                # Имя разбираемой функции
                print(func)
                for i, instr in enumerate(ret_attr):
                    # i - порядковый номер (чтобы не запутаться)
                    # instr - Instruction(opname='LOAD_ATTR', opcode=106, arg=2, argval='listen_port',
                    # argrepr='listen_port', offset=8, starts_line=None, is_jump_target=False)
                    # opername - имя для операции
                    print(i, instr)
                    if instr.opname == 'LOAD_GLOBAL':
                        if instr.argval not in methods:
                            # заполняем список методами, использующимися в функциях класса
                            methods.append(instr.argval)
                    elif instr.opname == 'LOAD_ATTR':
                        if instr.argval not in attrs:
                            # заполняем список атрибутами, использующимися в функциях класса
                            attrs.append(instr.argval)
        print(methods)
        # Если обнаружено использование недопустимого метода connect, бросаем исключение:
        if 'connect' in methods:
            raise TypeError('Использование метода connect недопустимо в серверном классе')
        # Если сокет не инициализировался константами SOCK_STREAM(TCP) AF_INET(IPv4), тоже исключение.
        if not ('SOCK_STREAM' in attrs and 'AF_INET' in attrs):
            raise TypeError('Проверьте сокет - он будто не инициализирован.')
        # Обязательно вызываем конструктор родителя:
        super().__init__(clssname, parents, attributes)


class ClientInspector(type):
    """
    Метакласс для проверки корректности клиентов.
    """
    def __init__(cls, clssname, parents, attributes):
        # Список методов, которые используются в функциях класса:
        methods = []
        for func in attributes:
            try:
                ret_attr = get_instructions(attributes[func])
                # Если не функция то ловим исключение
            except TypeError:
                pass
            else:
                # Раз функция разбираем код, получая используемые методы.
                for i in ret_attr:
                    if i.opname == 'LOAD_GLOBAL':
                        if i.argval not in methods:
                            methods.append(i.argval)
        # Если обнаружено использование недопустимого метода accept, listen (socket - не запрещен) бросаем исключение:
        for command in ('accept', 'listen'):
            if command in methods:
                raise TypeError('В классе обнаружены запрещенные методы!')
        # Вызов get_message или send_message из utils считаем корректным использованием сокетов
        if 'get_message' in methods or 'send_message' in methods:
            raise TypeError('Функции должны работать с JSON_Messenger!')
        else:
            pass
        super().__init__(clssname, parents, attributes)
