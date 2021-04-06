import logging
logger = logging.getLogger('server')


# Дескриптор для описания порта:
class CheckPort:
    def __set__(self, instance, value):
        # instance - <__main__.Server object at 0x000000D582740C50>
        # value - 7777
        try:
            port = int(value)
        except ValueError:
            raise ValueError('Порт должен быть целым числом!')
        if not 1023 < value < 65536:
            raise ValueError(
                f'Попытка запуска сервера с указанием неподходящего порта {value}. Допустимы адреса с 1024 до 65535.')
        # Если порт прошел проверку, добавляем его в список атрибутов экземпляра
        instance.__dict__[self.name] = value

    def __set_name__(self, owner, name):
        # owner - <class '__main__.Server'>
        # name - port
        self.name = name


# Дескриптор для описания IP (домена-то у меня нет...):
class CheckHost:
    def __set__(self, instance, value):
        start_ip = value.split('.')  # разделяю каждое число в IP, разделитель - точка
        max_oct = 254  # максимальное число в октете
        min_oct = 0  # минимальное число в октете
        oct_in_ip = 4  # кол-во октетов в ip

        # далее перевожу все числа в int
        start_ip[0] = int(start_ip[0])
        start_ip[1] = int(start_ip[1])
        start_ip[2] = int(start_ip[2])
        start_ip[3] = int(start_ip[3])
        # смотрю, что каждое число не больше 254 и не меньше 0
        i = 0
        if len(start_ip) == oct_in_ip:
            while i < oct_in_ip:  # !!!
                if not (start_ip[i] <= max_oct and start_ip[i] >= min_oct):
                    raise ValueError(
                f'Попытка запуска сервера с указанием неподходящего {i+1} октета IP {start_ip[i]}. '
                'Допустим диапазон с 0 до 254.')
                i += 1
            # если не ушло в ошибку ValueError, то октеты корректные
            instance.__dict__[self.name] = value
        else:
            raise ValueError('Октетов должно быть 4!')

    def __set_name__(self, owner, name):
        # owner - <class '__main__.Server'>
        # name - port
        self.name = name

