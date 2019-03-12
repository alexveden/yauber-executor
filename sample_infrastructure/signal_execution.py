"""
Signal execution script, routes orders across 2 brokers using RPC
"""
from yauber_executor import AsyncApp, log
import asyncio


class SignalExecutionScript(AsyncApp):

    async def on_new_signal(self, topic, message):
        print(f'on_new_signal: {topic}  {message}')

        brokers = ['broker1', 'broker2']
        accounts = ['acc1', 'acc2']

        orders = []

        for _broker, _acc in zip(brokers, accounts):
            orders.append(
                    self.call_rpc(f'{_broker}.execute', account=_acc, ticker=message['ticker'], price=message['price'], qty=message['qty'])
            )
        # Run async across all brokers
        results = await asyncio.gather(*orders)

        # Store execution results
        for exec_rec in results:
            print(exec_rec)

    def ampq_bind_funcs(self):
        return {
                'strategy.signal.*': self.on_new_signal,
        }

    async def main(self):
        log.info("Signal execution script is running")


if __name__ == '__main__':
    app = SignalExecutionScript(app_name='Signal execution')
    app.run()
