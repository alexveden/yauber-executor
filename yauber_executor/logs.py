import logging
import os
from datetime import datetime


class YaUberLogger(logging.Logger):
    def __init__(self, global_log_name, log_base_dir='logs'):
        super().__init__(global_log_name)

        self.logger_name = 'yauber_framework'
        self.logger_class = 'framework'
        self.logger = self

        #
        # Global settings
        #
        self.base_dir = log_base_dir
        self.log_level = logging.DEBUG
        self.setup(self.logger_class, self.logger_name)

    def setup(self, logger_class, name, to_file=False, log_level=None, file_mode='a'):
        self.logger_class = logger_class

        if to_file:
            self.log_dir = self.base_dir
            try:
                os.mkdir(self.log_dir)
            except FileExistsError:
                pass

            try:
                os.mkdir(os.path.join(self.log_dir, f'{datetime.now():%Y-%m-%d}'))
            except FileExistsError:
                pass

            try:
                os.mkdir(os.path.join(self.log_dir, f'{datetime.now():%Y-%m-%d}', self.logger_class))
            except FileExistsError:
                pass

        for hdlr in self.logger.handlers[:]:
            try:
                # Closing file descriptors for logs
                if isinstance(hdlr, logging.FileHandler):
                    hdlr.stream.close()
            except:
                pass

            self.logger.removeHandler(hdlr)

        formatter = logging.Formatter(fmt=f'%(asctime)s [%(filename)s:%(lineno)s] %(levelname)s - %(message)s')

        handler_console = logging.StreamHandler()
        handler_console.setFormatter(formatter)

        if to_file:
            handler_file = logging.FileHandler(os.path.join(self.log_dir,
                                                            f'{datetime.now():%Y-%m-%d}',
                                                            self.logger_class,
                                                            f'{name}.log'),
                                               mode=file_mode
                                               )
            handler_file.setFormatter(formatter)
            self.logger.addHandler(handler_file)

        if log_level is None:
            self.logger.setLevel(self.log_level)
        else:
            self.logger.setLevel(log_level)
        self.logger.addHandler(handler_console)


log = YaUberLogger('yauber_executor', log_base_dir=os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'logs'))