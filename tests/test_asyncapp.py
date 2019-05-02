import unittest
import asyncio
import pickle
from aio_pika import connect, Message, DeliveryMode, ExchangeType, IncomingMessage
import aio_pika.patterns
import aiormq.exceptions
from yauber_executor.asyncapp import *
from yauber_executor.logs import log
import unittest
from unittest import mock
import motor.motor_asyncio
import asyncio
import pamqp.specification
import logging


def _run(coro):
    """
    For execution of asyncio functions
    :param coro:
    :return:
    """
    return asyncio.get_event_loop().run_until_complete(coro)


def AsyncMock(*args, **kwargs):
    m = mock.MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    return mock_coro


class AsyncAppTestCase(unittest.TestCase):
    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    def test_init(self, mock_motor_client):
        app = AsyncApp('test_app', mongo_connstr='mongo_connstr', mongo_db='mongo_db',
                       ampq_connstr='ampq_conn', ampq_exchange='ampq_exchange', heartbeat_interval=120)

        self.assertEqual(app.app_name, 'test_app')
        self.assertEqual(app.ampq_connstr, 'ampq_conn')
        self.assertEqual(app.ampq_connection, None)
        self.assertEqual(app.ampq_channel, None)
        self.assertEqual(app.ampq_exchange_name, 'ampq_exchange')
        self.assertEqual(app.ampq_exchange_obj, None)
        self.assertEqual(app.ampq_rpc, None)
        self.assertEqual(app.ampq_isconnected, False)
        self.assertEqual(app.heartbeat_interval, 120)
        self.assertEqual(app._ampq_binded_funcs, {})
        self.assertEqual(app._is_shutting_down, False)
        self.assertEqual(app.loop, None)
        self.assertEqual(mock_motor_client.call_args[0], ('mongo_connstr',))
        self.assertEqual(mock_motor_client.mock_calls[1][0], '().__getitem__')
        self.assertEqual(mock_motor_client.mock_calls[1][1], ('mongo_db',))

        pass

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__logging_setup(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        with mock.patch.object(log, 'setup') as mock_log:
            app.logging_setup('test', 'app', logging.INFO, False, 'w')
            self.assertEqual(mock_log.call_args[0], ('test', 'app', False, logging.INFO, 'w'))

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    def test_ampq_bind_funcs(self, mock_motor_client):
        app = AsyncApp('test_app', mongo_connstr='mongo_connstr', mongo_db='mongo_db',
                       ampq_connstr='ampq_conn', ampq_exchange='ampq_exchange', heartbeat_interval=120)
        self.assertEqual(app.ampq_bind_funcs(), {})

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    def test_ampq_rpc_funcs(self, mock_motor_client):
        app = AsyncApp('test_app', mongo_connstr='mongo_connstr', mongo_db='mongo_db',
                       ampq_connstr='ampq_conn', ampq_exchange='ampq_exchange', heartbeat_interval=120)
        self.assertEqual(app.ampq_rpc_funcs(), {})

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__heartbeat(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        with mock.patch.object(AsyncApp, 'on_heartbeat', new=AsyncMock()) as mock_on_heartbeat, \
             mock.patch('asyncio.sleep', new=AsyncMock()) as mock_aio_sleep, \
             mock.patch('motor.core.AgnosticCollection.update_one', new=AsyncMock()) as mock_update_one:
                _mock_update_one = mock.MagicMock()
                _mock_update_one.update_one = mock_update_one
                app.mongo_db.__getitem__.return_value = _mock_update_one

                # Magic trick to stop endless loop!
                def onhbt(_self):
                    _self._is_shutting_down = True
                mock_on_heartbeat.mock.side_effect = onhbt

                _run(app._heartbeat())

                self.assertEqual(mock_on_heartbeat.mock.called, True)
                self.assertEqual(mock_update_one.mock.called, True)

                self.assertEqual(mock_update_one.mock.call_args[0][0], {'_id': 'test_app'})

                self.assertTrue('$set' in mock_update_one.mock.call_args[0][1])
                self.assertTrue('heartbeat_date_utc' in mock_update_one.mock.call_args[0][1]['$set'])

                self.assertEqual(mock_aio_sleep.mock.call_args[0], (111,))

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__heartbeat_exception(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        with mock.patch.object(AsyncApp, 'on_heartbeat', new=AsyncMock()) as mock_on_heartbeat, \
            mock.patch.object(log, 'exception') as mock_log, \
            mock.patch('asyncio.sleep', new=AsyncMock()) as mock_aio_sleep, \
            mock.patch('motor.core.AgnosticCollection.update_one', new=AsyncMock()) as mock_update_one:

                _mock_update_one = mock.MagicMock()
                _mock_update_one.update_one = mock_update_one
                app.mongo_db.__getitem__.return_value = _mock_update_one

                # Magic trick to stop endless loop!
                def onhbt(_self):
                    _self._is_shutting_down = True
                    raise ValueError('test')

                mock_on_heartbeat.mock.side_effect = onhbt

                _run(app._heartbeat())
                self.assertEqual(mock_log.called, True)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_on_heartbear(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)
        result = _run(app.on_heartbeat())

        self.assertEqual(result, None)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_main(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)
        result = _run(app.main())

        self.assertEqual(result, None)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_send_message(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        app.ampq_exchange_obj = mock.MagicMock()
        app.ampq_exchange_obj.publish = AsyncMock()
        app.ampq_exchange_obj.publish.mock.return_value = pamqp.specification.Basic.Ack()

        msg = {'msg': 'test'}
        _run(app.send_message('test.test', msg))

        _pikamsg = Message(pickle.dumps(msg), delivery_mode=DeliveryMode.PERSISTENT)

        self.assertEqual(app.ampq_exchange_obj.publish.mock.call_args[0][0].info(), _pikamsg.info())
        self.assertEqual(app.ampq_exchange_obj.publish.mock.call_args[1], {'routing_key': 'test.test'})

        # Delivery error
        app.ampq_exchange_obj.publish.mock.return_value = None
        self.assertRaises(AsyncAppDeliveryError, _run, app.send_message('test.test', msg))

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_rpc_call(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        app.ampq_rpc = mock.MagicMock()
        app.ampq_rpc.call = AsyncMock()

        _run(app.call_rpc('testfunc', **{'a':1}))

        self.assertEqual(app.ampq_rpc.call.mock.call_args[0], ('testfunc',))
        self.assertEqual(app.ampq_rpc.call.mock.call_args[1], {'kwargs': {'a':1}})

        # Error unregistered function
        def raise_exc():
            raise aiormq.exceptions.DeliveryError('test', 'test')
        app.ampq_rpc.call.mock.side_effect = lambda func_name, **kwargs: raise_exc()
        self.assertRaises(AsyncAppDeliveryError, _run, app.call_rpc('testfunc', **{'a':1}))

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_send_status(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        app.ampq_rpc = mock.MagicMock()
        app.ampq_rpc.call = AsyncMock()

        with mock.patch('motor.core.AgnosticCollection.replace_one', new=AsyncMock()) as mock_replace_one, \
             mock.patch.object(AsyncApp, 'send_message', new=AsyncMock()) as mock_send_message, \
             mock.patch.object(log, 'error') as mock_log_error, \
             mock.patch.object(log, 'warning') as mock_log_warning:

            _mock_replace_one = mock.MagicMock()
            _mock_replace_one.replace_one = mock_replace_one
            app.mongo_db.__getitem__.return_value = _mock_replace_one

            _run(app.send_status(AppStatus.ERROR, 'test_status'))
            # Collection name
            self.assertEqual('app_status', app.mongo_db.__getitem__.call_args[0][0])


            # Mongo replace_one
            repl_call = mock_replace_one.mock.call_args
            self.assertEqual(repl_call[0][0], {'_id': 'test_app'})
            status_dict = repl_call[0][1]

            self.assertEqual('test_app', status_dict['_id'])
            self.assertEqual(AppStatus.ERROR, status_dict['status'])
            self.assertEqual('test_status', status_dict['message'])
            self.assertEqual(0, int((datetime.now()-status_dict['date']).total_seconds()))
            self.assertEqual(0, int((datetime.utcnow() - status_dict['date_utc']).total_seconds()))
            self.assertEqual(0, int((datetime.utcnow() - status_dict['heartbeat_date_utc']).total_seconds()))

            self.assertEqual({'upsert': True}, repl_call[1])
            self.assertEqual(mock_send_message.mock.call_args[0], (app, 'status.test_app', status_dict))

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_send_status_error_handling_mongo(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        app.ampq_rpc = mock.MagicMock()
        app.ampq_rpc.call = AsyncMock()

        with mock.patch('motor.core.AgnosticCollection.replace_one', new=AsyncMock()) as mock_replace_one, \
             mock.patch.object(AsyncApp, 'send_message', new=AsyncMock()) as mock_send_message, \
             mock.patch.object(log, 'error') as mock_log_error, \
             mock.patch.object(log, 'warning') as mock_log_warning:

            # Errors
            def raise_exc():
                raise KeyError('Some generic error' )

            mock_replace_one.mock.side_effect = lambda *args, **kwargs: raise_exc()
            _run(app.send_status(AppStatus.ERROR, 'test_status'))
            self.assertEqual(mock_log_error.called, True)


    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_send_status_error_handling_ampq(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        app.ampq_rpc = mock.MagicMock()
        app.ampq_rpc.call = AsyncMock()

        with mock.patch('motor.core.AgnosticCollection.replace_one', new=AsyncMock()) as mock_replace_one, \
             mock.patch.object(AsyncApp, 'send_message', new=AsyncMock()) as mock_send_message, \
             mock.patch.object(log, 'exception') as mock_log_error, \
             mock.patch.object(log, 'warning') as mock_log_warning:

            _mock_replace_one = mock.MagicMock()
            _mock_replace_one.replace_one = mock_replace_one
            app.mongo_db.__getitem__.return_value = _mock_replace_one

            # Errors
            def raise_exc():
                raise KeyError('Some generic error' )

            def raise_exc_delivery():
                raise AsyncAppDeliveryError('test err')

            mock_send_message.mock.side_effect = lambda *args, **kwargs: raise_exc()
            _run(app.send_status(AppStatus.ERROR, 'test_status'))
            self.assertEqual(mock_log_error.called, True)

            mock_log_error.reset_mock()
            mock_send_message.mock.side_effect = lambda *args, **kwargs: raise_exc_delivery()
            _run(app.send_status(AppStatus.ERROR, 'test_status'))
            self.assertEqual(mock_log_error.called, False)
            self.assertEqual(mock_log_warning.called, True)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__process_message_valid_topic(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        mock_binded_func = AsyncMock()
        app._ampq_binded_funcs['test.app'] = mock_binded_func

        msg = mock.MagicMock(IncomingMessage)
        msg.body = pickle.dumps({'a': True})
        msg.routing_key = 'test.app'

        _run(app._process_message(msg))

        self.assertEqual(mock_binded_func.mock.call_args[0], ('test.app', {'a': True}))


    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__process_message_valid_topic_regex_matched(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        mock_binded_func = AsyncMock()
        app._ampq_binded_funcs['test.#'] = mock_binded_func

        msg = mock.MagicMock(IncomingMessage)
        msg.body = pickle.dumps({'a': True})
        msg.routing_key = 'test.app'

        _run(app._process_message(msg))

        self.assertEqual(mock_binded_func.mock.call_args[0], ('test.app', {'a': True}))

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__process_message_valid_topic_regex_mismatch(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        mock_binded_func = AsyncMock()
        app._ampq_binded_funcs['test.custom'] = mock_binded_func

        msg = mock.MagicMock(IncomingMessage)
        msg.body = pickle.dumps({'a': True})
        msg.routing_key = 'test.app'

        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status:
            _run(app._process_message(msg))
            self.assertEqual(mock_send_status.mock.call_args[0][1], AppStatus.ERROR)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__process_message_error_appmessageprocess(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        mock_binded_func = AsyncMock()
        app._ampq_binded_funcs['test.app'] = mock_binded_func

        msg = mock.MagicMock(IncomingMessage)
        msg.body = pickle.dumps({'a': True})
        msg.routing_key = 'test.app'

        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status:
            def raises_exc():
                raise AsyncAppMessageProcessError()

            mock_binded_func.mock.side_effect = lambda *args, **kwargs: raises_exc()

            _run(app._process_message(msg))
            self.assertEqual(mock_send_status.mock.call_args[0][1], AppStatus.ERROR)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__process_message_error_cancellederror(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        mock_binded_func = AsyncMock()
        app._ampq_binded_funcs['test.app'] = mock_binded_func

        msg = mock.MagicMock(IncomingMessage)
        msg.body = pickle.dumps({'a': True})
        msg.routing_key = 'test.app'

        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status, \
        mock.patch.object(AsyncApp, 'shutdown', new=AsyncMock()) as mock_shutdown:
            def raises_exc():
                raise asyncio.CancelledError()

            mock_binded_func.mock.side_effect = lambda *args, **kwargs: raises_exc()

            _run(app._process_message(msg))
            self.assertEqual(mock_send_status.mock.called, False)
            self.assertEqual(mock_shutdown.mock.called, False)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__process_message_error_generic_unhandled_error(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        mock_binded_func = AsyncMock()
        app._ampq_binded_funcs['test.app'] = mock_binded_func

        msg = mock.MagicMock(IncomingMessage)
        msg.body = pickle.dumps({'a': True})
        msg.routing_key = 'test.app'

        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status, \
                mock.patch.object(AsyncApp, 'shutdown', new=AsyncMock()) as mock_shutdown:
            def raises_exc():
                raise ValueError('Unhandled generic err')

            mock_binded_func.mock.side_effect = lambda *args, **kwargs: raises_exc()

            _run(app._process_message(msg))
            self.assertEqual(mock_send_status.mock.call_args[0][1], AppStatus.CRIT)
            self.assertEqual(mock_shutdown.mock.called, True)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__process_message_error_message_unpickle_error(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        mock_binded_func = AsyncMock()
        app._ampq_binded_funcs['test.app'] = mock_binded_func

        msg = mock.MagicMock(IncomingMessage)
        msg.body = {'a': True}
        msg.routing_key = 'test.app'

        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status, \
                mock.patch.object(AsyncApp, 'shutdown', new=AsyncMock()) as mock_shutdown:
            def raises_exc():
                raise ValueError('Unhandled generic err')

            mock_binded_func.mock.side_effect = lambda *args, **kwargs: raises_exc()

            _run(app._process_message(msg))
            self.assertEqual(mock_send_status.mock.call_args[0][1], AppStatus.ERROR)
            self.assertEqual(mock_shutdown.mock.called, False)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__process_message_error_message_unpickle_error2(self, mock_motor_client):
        app = AsyncApp('test_app', heartbeat_interval=111)

        mock_binded_func = AsyncMock()
        app._ampq_binded_funcs['test.app'] = mock_binded_func

        msg = mock.MagicMock(IncomingMessage)
        msg.body = b'123123123'
        msg.routing_key = 'test.app'

        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status, \
                mock.patch.object(AsyncApp, 'shutdown', new=AsyncMock()) as mock_shutdown:
            def raises_exc():
                raise ValueError('Unhandled generic err')

            mock_binded_func.mock.side_effect = lambda *args, **kwargs: raises_exc()

            _run(app._process_message(msg))
            self.assertEqual(mock_send_status.mock.call_args[0][1], AppStatus.ERROR)
            self.assertEqual(mock_shutdown.mock.called, False)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__amqp_pattern_match(self, mock_motor_client):
        # def _amqp_pattern_match(key: str, pattern: str) -> bool:
        self.assertEqual(True, AsyncApp._amqp_pattern_match('test.app', 'test.app'))
        self.assertEqual(False, AsyncApp._amqp_pattern_match('test.custom', 'test.app'))

        self.assertEqual(True, AsyncApp._amqp_pattern_match('test.custom', 'test.*'))
        self.assertEqual(False, AsyncApp._amqp_pattern_match('test.', 'test.*'))
        self.assertEqual(False, AsyncApp._amqp_pattern_match('test.custom.test', 'test.*'))

        self.assertEqual(True, AsyncApp._amqp_pattern_match('test.custom', 'test.#'))
        self.assertEqual(True, AsyncApp._amqp_pattern_match('test.custom.test', 'test.#'))
        self.assertEqual(True, AsyncApp._amqp_pattern_match('test.custom.', 'test.#'))
        self.assertEqual(False, AsyncApp._amqp_pattern_match('test', 'test.#'))
        self.assertEqual(False, AsyncApp._amqp_pattern_match('test', 'test#'))

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__ampq_connect_real_server(self, mock_motor_client):

        loop = asyncio.get_event_loop()
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        _run(app._ampq_connect(loop))

        self.assertEqual("amqp://guest:guest@localhost/", str(app.ampq_connection.connection.url))

        self.assertEqual('test_exchange', app.ampq_exchange_obj.name)
        self.assertEqual('topic', app.ampq_exchange_obj._Exchange__type)

        self.assertEqual(True, isinstance(app.ampq_rpc, aio_pika.patterns.RPC))
        self.assertEqual(True, app.ampq_isconnected)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__ampq_bind_topics_real_server(self, mock_motor_client):
        loop = asyncio.get_event_loop()
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        _run(app._ampq_connect(loop))

        app.ampq_channel = mock.MagicMock()
        app.ampq_channel.declare_queue = AsyncMock()

        mock_que_bind = mock.MagicMock()
        mock_que_bind.bind = AsyncMock()
        mock_que_bind.consume = AsyncMock()

        app.ampq_channel.declare_queue.mock.return_value = mock_que_bind


        with mock.patch.object(app, 'ampq_bind_funcs') as mock_ampq_bind_funcs:
            async def f1():
                pass
            async def f2():
                pass
            mock_ampq_bind_funcs.return_value = {
                'test.f1': f1,
                'test.f2': f2,
            }
            _run(app._ampq_bind_topics())

            self.assertEqual(app._ampq_binded_funcs['test.f1'], f1)
            self.assertEqual(app._ampq_binded_funcs['test.f2'], f2)
            self.assertEqual(mock_que_bind.bind.mock.call_args_list[0][1], {'routing_key': 'test.f1'})
            self.assertEqual(mock_que_bind.bind.mock.call_args_list[1][1], {'routing_key': 'test.f2'})

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__ampq_bind_topics_real_server_notconnected_error(self, mock_motor_client):
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")
        self.assertRaises(RuntimeError, _run, app._ampq_bind_topics())

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__ampq_bind_topics_real_server_consuming(self, mock_motor_client):
        loop = asyncio.get_event_loop()
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")
        _run(app._ampq_connect(loop))

        with mock.patch.object(app, 'ampq_bind_funcs') as mock_ampq_bind_funcs:
            async def f1():
                pass

            async def f2():
                pass

            mock_ampq_bind_funcs.return_value = {
                'test.f1': f1,
                'test.f2': f2,
            }
            _run(app._ampq_bind_topics())

            self.assertEqual(app._ampq_binded_funcs['test.f1'], f1)
            self.assertEqual(app._ampq_binded_funcs['test.f2'], f2)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__ampq_register_rpc_real_server(self, mock_motor_client):
        loop = asyncio.get_event_loop()
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")
        # Not connected raises
        self.assertRaises(RuntimeError, _run, app._ampq_register_rpc())


        #
        _run(app._ampq_connect(loop))
        with mock.patch.object(app, 'ampq_rpc_funcs') as mock_ampq_rpc_funcs:
            def add2(*, x=0, y=0):
                log.info(x)
                return x+y

            def mul(*, a=0, z=0):
                return a*z

            async def div(*, a=0, z=0):
                return a/z

            mock_ampq_rpc_funcs.return_value = {
                'test.add': add2,
                'test.mul': mul,
                'test.div': div,
            }
            _run(app._ampq_register_rpc())

            self.assertEqual(12, _run(app.call_rpc('test.mul', a=3, z=4)))
            self.assertEqual(6, _run(app.call_rpc('test.add', x=3, y=3)))
            self.assertRaises(ZeroDivisionError, _run, app.call_rpc('test.div', a=1, z=0))

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_shutdown_already_shutting_down(self, mock_motor_client):
        loop = asyncio.get_event_loop()
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        app._is_shutting_down = True
        app.loop = mock.MagicMock()
        _run(app.shutdown(sig=None))

        self.assertEqual(app.loop.stop.called, False)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_shutdown(self, mock_motor_client):
        loop = asyncio.get_event_loop()
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        app.loop = mock.MagicMock(loop)
        app.ampq_connection = mock.MagicMock()
        app.ampq_connection.close = AsyncMock()
        _run(app.shutdown(sig=None))

        self.assertEqual(True, app._is_shutting_down)
        self.assertEqual(True, app.loop.stop.called)
        #self.assertEqual(True, app.loop.close.called)
        self.assertEqual(True, app.mongo_client.close.called)
        self.assertEqual(True, app.ampq_connection.close.mock.called)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_shutdown_signal(self, mock_motor_client):
        loop = asyncio.get_event_loop()
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")
        app.loop = loop
        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status:
            app.loop.create_task(app.shutdown(sig=signal.SIGINT))
            app.loop.run_forever()
            self.assertEqual(mock_send_status.mock.call_args[0][1], AppStatus.OFF)


    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_shutdown_mongo_ampq_exception(self, mock_motor_client):
        loop = asyncio.get_event_loop()
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        app.loop = mock.MagicMock(loop)
        app.ampq_connection = mock.MagicMock()
        app.ampq_connection.close = AsyncMock()
        def _exc():
            raise RuntimeError()
        app.mongo_client.close.side_effect = _exc
        app.ampq_connection.close.mock.side_effect = _exc

        _run(app.shutdown(sig=None))

        self.assertEqual(True, app._is_shutting_down)
        self.assertEqual(True, app.loop.stop.called)
        self.assertEqual(True, app.mongo_client.close.called)
        self.assertEqual(True, app.ampq_connection.close.mock.called)


    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_shutdown_cancel_tasks(self, mock_motor_client):
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        async def task1():
            print('task1')
            await asyncio.sleep(5)

        async def task2():
            try:
                print('task2')
                await asyncio.sleep(10)
            except Exception as exc:
                print(exc)
                await asyncio.sleep(1)

        async def task3():
            try:
                print('task3')
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise ValueError('Expected cancelled')

        app.loop = asyncio.get_event_loop()

        t1 = app.loop.create_task(task1())
        t2 = app.loop.create_task(task2())
        t3 = app.loop.create_task(task3())
        app.loop.create_task(app._run_handler(app.shutdown(), start_delay=1))
        app.loop.run_forever()

        self.assertEqual(t1._state, 'CANCELLED')
        self.assertEqual(t2._state, 'FINISHED')
        self.assertEqual(t3._state, 'FINISHED')

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__run_handler(self, mock_motor_client):
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        with mock.patch('asyncio.sleep', new=AsyncMock()) as mock_aio_sleep:
            coro = AsyncMock()
            _run(app._run_handler(coro('test', True, arg1='b'), start_delay=0))
            self.assertEqual(False, mock_aio_sleep.mock.called)
            self.assertEqual(coro.mock.call_args[0], ('test', True))
            self.assertEqual(coro.mock.call_args[1], {'arg1': 'b'})

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__run_handler_start_delay(self, mock_motor_client):
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        with mock.patch('asyncio.sleep', new=AsyncMock()) as mock_aio_sleep:
            coro = AsyncMock()
            _run(app._run_handler(coro('test', True, arg1='b'), start_delay=1))
            self.assertEqual(True, mock_aio_sleep.mock.called)
            self.assertEqual((1,), mock_aio_sleep.mock.call_args[0])
            self.assertEqual(coro.mock.call_args[0], ('test', True))
            self.assertEqual(coro.mock.call_args[1], {'arg1': 'b'})

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__run_handler_exceptions(self, mock_motor_client):
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        with mock.patch('asyncio.sleep', new=AsyncMock()) as mock_aio_sleep, \
            mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status, \
            mock.patch.object(AsyncApp, 'shutdown', new=AsyncMock()) as mock_shutdown:

                async def task():
                    await asyncio.sleep(1)
                    raise ValueError('expected error')

                _run(app._run_handler(task(), start_delay=0))

                self.assertEqual(True, mock_shutdown.mock.called)
                self.assertEqual(mock_send_status.mock.call_args[0][1], AppStatus.CRIT)


    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test__run_handler_exceptions_cancellation(self, mock_motor_client):
        app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status:

            async def task1():
                print("task1")
                await asyncio.sleep(2)
                raise ValueError('expected error')

            async def task2():
                print("task2")
                while True:
                    await asyncio.sleep(1)

            app.loop = asyncio.get_event_loop()

            app.loop.create_task(app._run_handler(task1(), start_delay=0))
            app.loop.create_task(app._run_handler(task2(), start_delay=0))
            app.loop.run_forever()

            self.assertEqual(mock_send_status.mock.call_args[0][1], AppStatus.CRIT)

    @mock.patch('motor.motor_asyncio.AsyncIOMotorClient')
    # Patch arguments MUST be in reverse order!!!
    def test_run(self, mock_motor_client):


        with mock.patch.object(AsyncApp, 'send_status', new=AsyncMock()) as mock_send_status, \
             mock.patch.object(AsyncApp, '_heartbeat', new=AsyncMock()) as mock_heartbeat, \
             mock.patch.object(AsyncApp, '_ampq_connect', new=AsyncMock()) as mock__ampq_connect, \
             mock.patch.object(AsyncApp, '_ampq_bind_topics', new=AsyncMock()) as mock__ampq_bind_topics, \
             mock.patch.object(AsyncApp, '_ampq_register_rpc', new=AsyncMock()) as mock__ampq_register_rpc:

            app = AsyncApp('test_app', ampq_exchange='test_exchange', ampq_connstr="amqp://guest:guest@localhost/")

            async def _main():
                print('main')
                raise RuntimeError('End endless loop')

            app.main = _main

            app.run()

            self.assertEqual(mock_send_status.mock.call_args_list[0][0][1], AppStatus.IDLE)
            self.assertEqual(mock__ampq_connect.mock.call_args[0], (app, app.loop))
            self.assertEqual(mock__ampq_connect.mock.call_args[1], {})

            self.assertEqual(mock__ampq_bind_topics.mock.call_args[0], (app,))
            self.assertEqual(mock__ampq_bind_topics.mock.call_args[1], {})

            self.assertEqual(mock__ampq_register_rpc.mock.call_args[0], (app,))
            self.assertEqual(mock__ampq_register_rpc.mock.call_args[1], {})

            self.assertEqual(mock_heartbeat.mock.call_args[0], (app,))
            self.assertEqual(mock_heartbeat.mock.call_args[1], {})

            self.assertEqual(mock_send_status.mock.call_args_list[1][0][1], AppStatus.CRIT)

    def test_async_to_callback(self):

        async def add(x, y):
            raise RuntimeError()

        call_back = async_to_callback(add)

        async def _run_call():
            call_back(2,3)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(_run_call())





if __name__ == '__main__':
    unittest.main()