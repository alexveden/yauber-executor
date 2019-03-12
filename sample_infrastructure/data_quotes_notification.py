"""
Simple quotes notification script, fires a new quote event message for other scripts
"""
from yauber_executor import AsyncApp, log, AsyncAppDeliveryError
import asyncio


class QuotesScript(AsyncApp):
    async def main(self):
        log.info("Quotes script is running")
        tickers = ['SPY', 'AAPL']

        while True:
            log.info("New quote emitted")

            # Emit quotes for all tickers asynchronously every 60 seconds
            try:
                await asyncio.gather(*[
                                        self.send_message(f'data.quote.{ticker}', {'ticker': ticker})
                                        for ticker in tickers
                ])
            except AsyncAppDeliveryError:
                # no script listening the quotes feed, this is ok, just skip
                pass

            await asyncio.sleep(60)


if __name__ == '__main__':
    app = QuotesScript(app_name='QuotesScript')
    app.run()
