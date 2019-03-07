from ast import literal_eval
from pymongo import MongoClient
import sys
import logging
from .signalapp import SignalApp, MsgBase, MsgStatus
from datetime import datetime

COLLECTION_EVENTS = 'scripts_events'
COLLECTION_STATUS = 'scripts_status'
COLLECTION_STATE = 'scripts_state'

EVENT_DEBUG = "DEBUG"
EVENT_INFO = "INFO"
EVENT_WARN = "WARNING"
EVENT_ERROR = "ERROR"

STATUS_NA = "NA"
STATUS_OK = "OK"
STATUS_ERR = "ERROR"


class ScriptBase:
    """
    Utility script base class
    """
    script_name = 'ScriptBase'
    """Unique name of the script"""

    script_type = 'Generic'
    """Type of the script group"""

    def __init__(self, argv=None, **kwargs):
        client = MongoClient(MONGO_CONNSTR)
        self.db = client[MONGO_DB]
        self.kwargs = kwargs

        _console_args, _console_kwargs = self.parse_args(argv)
        self.args = _console_args
        self.kwargs.update(_console_kwargs)

        self.state = {}
        """Script calculation state"""

        # Setting up RabbitMQ messaging / notifications system
        self.signall_app = SignalApp(self.script_name, self.script_type)

        # Setting up framework wide logging engine
        log_level = logging.INFO
        if self.kwargs.get('log_debug', False):
            log_level = logging.DEBUG

        log.setup('scripts', f'{self.script_type}.{self.script_name}', to_file=True, log_level=log_level)

    def load_state(self):
        """
        Load script state from the DB
        :return: state dict
        """
        self.state = self.db[COLLECTION_STATE].find_one({'_id': self.script_name})
        if self.state is None:
            self.state = {}

    def save_state(self):
        """
        Saves script state to the DB
        :return:
        """
        self.state['_id'] = self.script_name
        self.db[COLLECTION_STATE].replace_one({'_id': self.script_name}, self.state, upsert=True)

    @staticmethod
    def parse_args(args=None):
        """Parse positional and keyword arguments from ``sys.argv`` or given list
        of *args*.
        :param args: list of string to parse, defaults to ``sys.argv[1:]``.
        :return: :class:`tuple` of positional args and :class:`dict` of keyword
            arguments.
        Positional arguments have no specific syntax. Keyword arguments must be
        written as ``--{keyword-name}={value}``::
            >>> parse_args(['1', 'hello', 'True', '3.1415926', '--force=True'])
            ((1, 'hello', True, 3.1415926), {'force': True})
        """

        def parse_literal(string):
            """Parse Python literal or return *string* in case :func:`ast.literal_eval`
            fails."""
            try:
                return literal_eval(string)
            except (ValueError, SyntaxError):
                return string

        if args is None:
            args = sys.argv[1:]

        positional_args, kwargs = (), {}
        for arg in args:
            if arg.startswith('--'):
                arg = arg[2:]
                try:
                    key, raw_value = arg.split('=', 1)
                    value = parse_literal(raw_value)
                except ValueError:
                    key = arg
                    value = True
                kwargs[key.replace('-', '_')] = value
            else:
                positional_args += (parse_literal(arg),)

        return positional_args, kwargs

    def run(self):
        """
        Run utility script
        :return:
        """
        pass

    def status(self, status_type, message):
        """
        Reports status for script
        :param status_type:
        :param message:
        :return:
        """
        self.db[COLLECTION_STATUS].insert_one({
            'script_name': self.script_name,
            'script_type': self.script_type,
            'date': datetime.utcnow(),
            'status': status_type,
            'message': message,
        })

        self.signall_app.send(MsgStatus(status_type, message, notify=True))

    def __str__(self):
        return f"{self.script_type}.{self.script_name}"

