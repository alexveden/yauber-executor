import unittest
from yauber_executor.logs import YaUberLogger, log
import logging
import os
from datetime import datetime
import shutil


class LoggerTestCase(unittest.TestCase):
    def test_init(self):
        logger = YaUberLogger('test', log_base_dir='test')

        self.assertEqual(logger.log_level, logging.DEBUG)
        self.assertEqual(logger.base_dir, 'test')
        self.assertEqual(logger.logger_class, 'framework')
        self.assertEqual(logger.logger_name, 'yauber_framework')

    def test_setup(self):

        logger = YaUberLogger('test', log_base_dir='test_logs')
        logger.setup('test', 'run1', to_file=True, log_level=logging.INFO, file_mode='a')
        logger.debug('debug')
        logger.info('info')
        logger.error('error')

        log_path = os.path.join('test_logs', f'{datetime.now():%Y-%m-%d}', 'test')
        with open(os.path.join(log_path, 'run1.log'), 'r') as f:
            text = f.read()
            self.assertTrue('debug' not in text)
            self.assertTrue('info' in text)
            self.assertTrue('error' in text)

        logger.setup('test', 'run2', to_file=True, log_level=logging.INFO, file_mode='w')
        logger.info('info')

        with open(os.path.join(log_path, 'run2.log'), 'r') as f:
            text = f.read()
            self.assertTrue('info' in text)

        logger.setup('test', 'run2', to_file=True, log_level=logging.INFO, file_mode='w')
        logger.critical('critical')
        with open(os.path.join(log_path, 'run2.log'), 'r') as f:
            text = f.read()
            self.assertTrue('info' not in text)
            self.assertTrue('critical' in text)

        shutil.rmtree('test_logs', ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
