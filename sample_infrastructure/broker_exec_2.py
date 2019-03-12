"""
Broker 2 Execution API
"""
from yauber_executor.asyncapp import AsyncApp
import asyncio
from yauber_executor.logs import log
import uuid


class Broker2Execution(AsyncApp):

    async def rpc_execute(self, *, account, ticker, price, qty):
        print(f'executing: Acc:{account} Ticker: {ticker} Price: {price} Qty: {qty}')
        await asyncio.sleep(1)

        return {'status': 'FILLED', 'orderid': str(uuid.uuid4()), 'broker': 'broker2'}

    def ampq_bind_funcs(self):
        return {
            # No topics listened
        }

    def ampq_rpc_funcs(self):
        return {
            'broker2.execute': self.rpc_execute,
        }

    async def on_heartbeat(self):
        # Every 30 seconds check the API connection status
        log.info("Checking API connection")

    async def main(self):
        log.info("Running Broker 2 API")


if __name__ == '__main__':
    app = Broker2Execution(app_name='Broker2Execution', heartbeat_interval=30)
    app.run()
