from decorator import Log, LOGGER
import sys


class AbstractServerConfig:
    address = None
    port = None
    db_path = None
    db_file = None


class DefaultServerConfig(AbstractServerConfig):
    def __init__(self):
        self.address = '127.0.0.1'
        self.port = 7777
        self.db_path = ''
        self.db_file = 'server_database.db3'


class ServerConfig(AbstractServerConfig):
    default = DefaultServerConfig()

    def read_default(self):
        self.address = self.default.address
        self.port = self.default.port
        self.db_path = self.default.db_path
        self.db_file = self.default.db_file

    def read_from_ini(self, ini):
        if ini.has_section('SETTINGS'):
            self.address = ini.get('SETTINGS', 'Default_address', fallback=self.default.address)
            self.port = ini.getint('SETTINGS', 'Default_port', fallback=self.default.port)
            self.db_path = ini.get('SETTINGS', 'Database_path', fallback=self.default.db_path)
            self.db_file = ini.get('SETTINGS', 'Database_file', fallback=self.default.db_file)

    def write_to_ini(self, ini):
        if not ini.has_section('SETTINGS'):
            ini.add_section('SETTINGS')
        ini['SETTINGS']['Default_port'] = self.address
        ini['SETTINGS']['Default_port'] = str(self.port)
        ini['SETTINGS']['Database_path'] = self.db_path
        ini['SETTINGS']['Database_file'] = self.db_file

    def read_from_cmd(self):
        """
        server.py -p 8079 -a 192.168.1.2
        """
        try:
            if '-p' in sys.argv:
                self.port = sys.argv[sys.argv.index('-p') + 1]
        except IndexError:
            LOGGER.critical('После параметра -\'p\' необходимо указать номер порта.')
            raise SystemError

        try:
            if '-a' in sys.argv:
                self.address = sys.argv[sys.argv.index('-a') + 1]
        except IndexError:
            LOGGER.critical('После параметра \'a\'- необходимо указать адрес, который будет слушать сервер.')
            raise SystemError

    def copy_to(self, config):
        config.address = self.address
        config.port = self.port
        config.db_path = self.db_path
        config.db_file = self.db_file

    def validate(self):
        try:
            self.port = int(self.port)
        except ValueError:
            raise ValueError('Порт должен быть числом')
        if not 1023 < self.port < 65536:
            raise ValueError('Порт должен быть числом от 1024 до 65535')
