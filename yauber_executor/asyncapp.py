import aio_pika
import motor
import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType
from aio_pika.patterns import RPC
from .logs import log
import pickle
import re
import signal
import pamqp.specification
import aiormq.exceptions
import motor.motor_asyncio
from datetime import datetime
from typing import Any, Dict, Coroutine


class AsyncAppError(Exception):
    pass


class AsyncAppConnectionError(AsyncAppError):
    pass


class AsyncAppDeliveryError(AsyncAppError):
    pass


class AsyncAppMessageProcessError(AsyncAppError):
    pass


class AppStatus:
    IDLE = 'IDLE'
    RUN = 'RUN'
    ERROR = 'ERROR'
    CRIT = 'CRIT'
    WARN = 'WARN'
    OFF = 'OFF'


def async_to_callback(coro):
    def callback(*args, **kwargs):
        asyncio.ensure_future(coro(*args, **kwargs))
    return callback


class AsyncApp:
    def __init__(self,
                 app_name,
                 ampq_connstr="amqp://guest:guest@localhost/",
                 ampq_exchange='yauber_executor',
                 mongo_db='yauber_executor',
                 mongo_connstr='mongodb://localhost',
                 heartbeat_interval=60,
                 ):
        self.app_name = app_name
        self.ampq_connstr = ampq_connstr
        self.ampq_connection = None
        self.ampq_channel = None
        self.ampq_exchange_name = ampq_exchange
        self.ampq_exchange_obj = None
        self.ampq_rpc = None
        self.ampq_isconnected = False
        self.heartbeat_interval = heartbeat_interval

        self._ampq_binded_funcs = {}
        self._is_shutting_down = False
        self.loop = None

        self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_connstr)
        self.mongo_db = self.mongo_client[mongo_db]
        self.state = {}

    def logging_setup(self, class_name, app_name, log_level, to_file=True, file_mode='a'):
        """
        Manage app logging
        :param class_name: global type of script (e.g. 'execution', 'data', 'core', etc)
        :param app_name: unique application name
        :param log_level: log verbosity (logging.DEBUG, logging.INFO, etc..)
        :param to_file: if True write to file
        :param file_mode: 'a' - for appending, 'w' - for rewriting
        :return: Nothing
        """
        log.setup(class_name, app_name, to_file, log_level, file_mode)

    def ampq_bind_funcs(self) -> Dict[str, Coroutine]:
        """
        Initial binding of AMQP message topics to processors
        :return:
        """
        return {}

    def ampq_rpc_funcs(self) -> Dict[str, Coroutine]:
        """
        Dictionary of system wide RPC function that script exposes
        :return:
        """
        return {}

    async def _heartbeat(self):
        while not self._is_shutting_down:
            try:
                res_dict = await self.on_heartbeat()
                values = {'heartbeat_date_utc': datetime.utcnow()}
                if res_dict is not None:
                    if not isinstance(res_dict, dict):
                        log.error('self.on_heartbeat() must return a dictionary or None')
                    else:
                        for k, v in res_dict.items():
                            values[k] = v

                await self.mongo_db['app_status'].update_one({'_id': self.app_name},
                                                             {'$set': values})
            except Exception as exc:
                log.exception(f"Error writing heartbeat data to the MongoDB. Exception: {exc}")

            await asyncio.sleep(self.heartbeat_interval)

    async def on_heartbeat(self) -> Dict[str, Any]:
        """
        Periodic script self-health check and status reporting
        :return: key/value dictionary, must have string keys without dots and values must be comparible with MongoDB collection types
        """
        return {}

    async def main(self):
        """
        Main function of the app
        :return:
        """
        pass

    async def initialize(self):
        """
        Initialization of child class of AsyncApp, this method is called before main() and any events subscription.
        Useful for class attributes definition
        :return:
        """
        pass

    async def send_message(self, topic: str, msg_obj: Any):
        """
        Send message across AMPQ
        :param topic: message topic
        :param msg_obj: any but must be eligible for pickling
        :return: Nothing
        :raises: AsyncAppDeliveryError - if no other apps listening
        """
        serialized_msg = pickle.dumps(msg_obj)
        message = Message(serialized_msg, delivery_mode=DeliveryMode.PERSISTENT)
        result = await self.ampq_exchange_obj.publish(message, routing_key=topic)

        if not isinstance(result, pamqp.specification.Basic.Ack):
            # Message is not delivered
            raise AsyncAppDeliveryError(f"Message is not delivered. AMPQ result: {result}")

    async def call_rpc(self, rpc_func_name, **kwargs) -> Any:
        """
        Call remote RPC function of other AsyncApp via AMPQ
        :param rpc_func_name: unique name of function
        :param kwargs: function kwargs
        :return: any
        :raises: Any exception raised by remote function
        :raises: AsyncAppDeliveryError - if no other apps listening
        """
        try:
            return await self.ampq_rpc.call(rpc_func_name, kwargs=kwargs)
        except aiormq.exceptions.DeliveryError:
            raise AsyncAppDeliveryError(f"Called unregistered RPC function: {rpc_func_name}")

    async def send_status(self, status, message):
        """
        Send status message via AMPQ
        :param status: AppStatus.<> constant
        :param message: any pickleable
        :return: nothing
        :raises: nothing (catches all) but writes to log
        """
        current_status = {
                '_id': self.app_name,
                'status': status,
                'message': message,
                'date': datetime.now(),
                'date_utc': datetime.utcnow(),
                'heartbeat_date_utc': datetime.utcnow(),
        }

        log.info(f"STATUS: {status} -> {message}")

        try:
            await self.mongo_db['app_status'].replace_one({'_id': self.app_name}, current_status, upsert=True)
        except Exception:
            log.error("Error writing status data to the MongoDB")

        try:
            await self.send_message(f'status.{self.app_name}', current_status)
        except AsyncAppDeliveryError:
            log.warning("AMPQ no status listeners")
        except Exception as exc:
            log.exception(f"Error during sending status: {exc}")

    async def _process_message(self, message):
        with message.process():

            topic = message.routing_key
            msg_obj = message.body
            processor_coro = "<undefined>"

            # Try message integrity
            try:
                msg_obj = pickle.loads(message.body)
            except:
                log.exception(f'Corrupted message format {message.body}')
                await self.send_status(AppStatus.ERROR, f'Corrupted message format, see logs...')
                return

            try:
                if topic in self._ampq_binded_funcs:
                    # Try fast search
                    processor_coro = self._ampq_binded_funcs[topic](topic, msg_obj)
                    await processor_coro
                else:
                    # Try find handler by regex
                    is_found = False
                    for bind_key, bind_coro in self._ampq_binded_funcs.items():
                        if self._amqp_pattern_match(topic, bind_key):
                            is_found = True
                            processor_coro = bind_coro(topic, msg_obj)
                            await processor_coro
                            break
                    if not is_found:
                        log.error(f'Routing function is not found for routing topic {topic}')
                        await self.send_status(AppStatus.ERROR, f'Routing function is not found for routing topic {topic}')
            except AsyncAppMessageProcessError as exc:
                log.error(f'Error in processing message\n'
                          f'\tTopic: {topic}\n'
                          f'\tProcessor: {processor_coro}\n'
                          f'\tException: {exc}')
                await self.send_status(AppStatus.ERROR, f'AsyncAppMessageProcessError: {topic}')
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception(f'Unhandled exception in processing topic:\n'
                              f'\tTopic: {topic}\n'
                              f'\tProcessor: {processor_coro}\n'
                              f'\tMessage Obj: {msg_obj}')
                await self.send_status(AppStatus.CRIT, f"Unhandled exception in processing topic: {topic}")
                await self.shutdown()

    @staticmethod
    def _amqp_pattern_match(key: str, pattern: str) -> bool:
        if key == pattern:
            return True
        replaced = pattern.replace(r'*', r'([^.]+)').replace(r'#', r'([^.]+.?)+')
        regex_string = f"^{replaced}$"
        match = re.search(regex_string, key)
        return match is not None

    async def _ampq_connect(self, loop):
        log.info("Connecting to AMPQ server")
        self.ampq_connection = await connect(self.ampq_connstr, loop=loop)

        # Creating a channel
        self.ampq_channel = await self.ampq_connection.channel()

        # Declare an exchange
        self.ampq_exchange_obj = await self.ampq_channel.declare_exchange(self.ampq_exchange_name, ExchangeType.TOPIC)

        # Register RPC channel
        self.ampq_rpc = await RPC.create(self.ampq_channel)

        self.ampq_isconnected = True

    async def _ampq_bind_topics(self):
        if not self.ampq_isconnected:
            raise RuntimeError('AMPQ is not connected')

        binding_keys_dict = self.ampq_bind_funcs()

        if binding_keys_dict is not None and len(binding_keys_dict) > 0:
            # Declaring queue
            queue = await self.ampq_channel.declare_queue(exclusive=True)

            # Bind topics to processor functions
            for binding_key, binding_coro in binding_keys_dict.items():
                await queue.bind(self.ampq_exchange_obj, routing_key=binding_key)
                log.debug(f'Listening to: {binding_key} on {binding_coro}')
                self._ampq_binded_funcs[binding_key] = binding_coro

            # Start listening the queue
            await queue.consume(async_to_callback(self._process_message))

    async def _ampq_register_rpc(self):
        if not self.ampq_isconnected:
            raise RuntimeError("AMPQ is not connected, no RPC applied")

        rpc_funcs_dict = self.ampq_rpc_funcs()

        if rpc_funcs_dict is not None and len(rpc_funcs_dict) > 0:

            for rpc_key, rpc_func in rpc_funcs_dict.items():
                log.debug(f"Registering RPC {rpc_key} -> {rpc_func}")

                await self.ampq_rpc.register(rpc_key,
                                             rpc_func,
                                             auto_delete=True, exclusive=True)

    async def shutdown(self, sig=None):
        if self._is_shutting_down:
            # Avoid concurrent shutdown
            return

        self._is_shutting_down = True
        if sig is not None:
            log.info(f'Received exit signal {sig.name}...')
            await self.send_status(AppStatus.OFF, f'Received exit signal {sig.name}')
        else:
            log.info(f'Received unhandled error, shutting down...')

        log.info('Closing connections')
        if self.ampq_connection is not None:
            try:
                await self.ampq_connection.close()
                log.info('AMPQ connection closed')
            except Exception as exc:
                log.error(f"AMPQ Connection close error: {exc}")

        if self.mongo_client is not None:
            try:
                self.mongo_client.close()
                log.info('MongoDB connection closed')
            except Exception as exc:
                log.error(f"MongoDB Connection close error: {exc}")

        log.info('Canceling outstanding tasks')
        tasks = []

        loop = self.loop

        for t in asyncio.Task.all_tasks(loop):
            if t is not asyncio.Task.current_task(loop):
                if t.cancel():
                    # Immediately after this method is called, cancelled() will not return True (unless the task was already cancelled).
                    tasks.append(t)

        if len(tasks) > 0:
            cancellation_results = await asyncio.gather(*tasks, return_exceptions=True)

            for t, result in zip(tasks, cancellation_results):
                if not isinstance(result, asyncio.CancelledError) and isinstance(result, Exception):
                    log.exception(f'{t._coro} Cancel ERROR:', exc_info=result)

        loop.stop()
        # loop.close()
        log.info('Shutdown complete.')

    async def _run_handler(self, coro, start_delay=0):
        try:
            if start_delay > 0:
                log.info(f'Delayed launch {start_delay}s of {coro}')
                await asyncio.sleep(start_delay)
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception(f'Unhandled exception in {coro}')

            if not self._is_shutting_down:
                await self.send_status(AppStatus.CRIT, 'Unhandled exception!')
                await self.shutdown()

    def run(self):
        self.loop = loop = asyncio.get_event_loop()
        loop.set_debug(True)

        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for sig in signals:
            loop.add_signal_handler(sig, lambda s=sig: loop.create_task(self.shutdown(s)))

        # Connect to RabbitMQ first and report initial status
        loop.run_until_complete(self._run_handler(self.initialize()))
        loop.run_until_complete(self._run_handler(self._ampq_connect(loop)))
        loop.run_until_complete(self.send_status(AppStatus.IDLE, 'Started...'))

        # Listen AMPQ events and register RPC calls if available
        loop.create_task(self._run_handler(self._ampq_bind_topics()))
        loop.create_task(self._run_handler(self._ampq_register_rpc()))

        # Run main routine and heartbeat
        loop.create_task(self._run_handler(self.main(), start_delay=5))
        loop.create_task(self._run_handler(self._heartbeat(), start_delay=2))

        loop.run_forever()

