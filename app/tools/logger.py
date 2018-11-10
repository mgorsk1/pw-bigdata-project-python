import logging

from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from os import path, makedirs
from socket import gethostname
from sys import exit, stdout
from config import BASE_PATH, LOG_LEVEL


class Logger:
    def __init__(self, name_arg, debug_level, rotate_retention_arg, rotate_when_arg='midnight'):
        self.name = name_arg
        self.path = "{}".format(BASE_PATH)
        self.debug_level = int(debug_level)
        self.log_dir = '{path}/log'.format(path=self.path)

        self.current_level = 0

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.debug_level)

        self.hostname = gethostname()

        self.rotate_when = rotate_when_arg
        self.rotate_retention = rotate_retention_arg

        self.levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR,
                       'CRITICAL': logging.CRITICAL}

        # create log dir
        try:
            if not path.exists(self.log_dir):
                makedirs(self.log_dir)
        except Exception as e:
            exit(1)

    def logit(self, t):
        try:
            log_level = t['log_level']
            log_message = t['log_message']

            if log_level == 'DEBUG':
                self.log_debug(log_message)
            elif log_level == 'INFO':
                self.log_info(log_message)
            elif log_level == 'WARNING':
                self.log_warning(log_message)
            elif log_level == 'ERROR':
                self.log_error(log_message)
            elif log_level == 'CRITICAL':
                self.log_critical(log_message)
            else:
                pass
        except:
            pass
        finally:
            pass

    def log_debug(self, message):
        self.change_formatter(logging.DEBUG)
        self.logger.debug(message)

    def log_info(self, message):
        self.change_formatter(logging.INFO)
        self.logger.info(message)

    def log_warning(self, message):
        self.change_formatter(logging.WARNING)
        self.logger.warning(message)

    def log_error(self, message):
        self.change_formatter(logging.ERROR)
        self.logger.error(message)

    def log_critical(self, message):
        self.change_formatter(logging.CRITICAL)
        self.logger.critical(message)

    def log_time_tune(self, message):
        self.change_formatter(logging.CRITICAL)
        self.logger.critical('TIME TUNING {msg}'.format(msg=message))

    def change_formatter(self, level_arg):
        template = '%(asctime)s %(levelname)s {host} %(process)d {repeat} > %(message)s'

        max_len = max([len(x) for x in self.levels.keys()]) + 1

        blank_space = {v: template.format(repeat='-' * (max_len - len(k)), host=self.hostname) for k, v in
                       self.levels.items()}

        # prevent changing formatter if using the same level of logging
        if self.current_level == level_arg:
            return
        else:
            self.current_level = level_arg

        # clear handlers
        try:
            if self.logger.handlers:
                self.logger.handlers = []
        except:
            pass

        # add handler
        log_file = '{path}/log/{name}.log'.format(path=self.path, name=self.name)

        # handler = logging.handlers.TimedRotatingFileHandler(log_file,
        #                                                     when=self.rotate_when,
        #                                                     interval=1,
        #                                                     backupCount=self.rotate_retention)

        handler = logging.StreamHandler(stdout)

        formatter = logging.Formatter(blank_space[level_arg])
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)



log = Logger('pw-bd-project', LOG_LEVEL, 30)
