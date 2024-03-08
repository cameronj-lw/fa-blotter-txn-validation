
from configparser import ConfigParser
from dataclasses import dataclass
import os
import socket


@dataclass
class AppConfig:
    # Default config file is config.ini in the "app" folder
    config_file_path: str = os.path.join(os.path.abspath(__file__), os.pardir, os.pardir, os.pardir, 'config.ini')

    def __post_init__(self):
        # If there is a config file specifically for this hostname, use it instead:
        hostname_specific_config_file_path = self.config_file_path.replace('config.ini', f'config-{socket.gethostname()}.ini')
        if os.path.exists(hostname_specific_config_file_path):
            self.config_file_path = hostname_specific_config_file_path

        # Now create parser
        self.parser = ConfigParser()
        self.parser.read(self.config_file_path)

    def get(self, *args, **kwargs):
        """ Syntactic sugar to facilitate AppConfig().get(...) rather than AppConfig().parser.get(...) """
        return self.parser.get(*args, **kwargs)

    def to_string(self):
        with open(self.config_file_path, 'r') as f:
            config_file_content = f.read()
        return config_file_content

