"""
Simple strategy calculation script, it's waiting for new quotes signals and launches strategy calculation
"""
from yauber_executor import AsyncApp, log, AsyncAppDeliveryError, AppStatus
import asyncio
import time
from datetime import datetime
import concurrent.futures


class HolyGrailStrategy:
    def __init__(self, strategy_name, tickers, calc_time=10):
        self.strategy_name = strategy_name
        self.tickers = tickers
        self.calc_time = calc_time

    def run(self):
        """
        Calculates strategy signals
        :return:
        """
        # Simulate CPU intensive task
        time.sleep(self.calc_time)

        # Return signals
        return [
                {'strategy': self.strategy_name,
                 'ticker': ticker,
                 'qty': 100*(i+1),
                 'price': 120+(i+1)
                } for i, ticker in enumerate(self.tickers)
        ]


class StrategyLauncherScript(AsyncApp):

    @staticmethod
    def run_strategy(StrategyClass, strategy_name, tickers, calc_time):
        print(f'Running {strategy_name} calc time: {calc_time}')
        strategy = StrategyClass(strategy_name, tickers, calc_time)
        return strategy.run()

    async def on_new_quote(self, topic, message):
        print(f'on_new_quote: {topic}  {message}')
        executor_pool = concurrent.futures.ProcessPoolExecutor(max_workers=3)

        run_strategies_tasks = []

        if message['ticker'] == 'SPY':
            # Launch SPY strategy
            run_strategies_tasks.append(
                    self.loop.run_in_executor(executor_pool, self.run_strategy, HolyGrailStrategy, 'spy_strat1', ['SPY'], 10)
            )
        elif message['ticker'] == 'AAPL':
            run_strategies_tasks.append(
                    self.loop.run_in_executor(executor_pool, self.run_strategy, HolyGrailStrategy, 'appl_strat1', ['AAPL'], 10)
            )
        last_quote_time = self.state.get('last_quote_time')

        if last_quote_time is None or last_quote_time.minute != datetime.now().minute:
            # Run portfolio based strategy
            # Prevent double launching of portfolio strategy
            run_strategies_tasks.append(
                    self.loop.run_in_executor(executor_pool, self.run_strategy, HolyGrailStrategy, 'portfolio_strat1', ['INTC', 'F', 'BAC'], 30)
            )

        self.state['last_quote_time'] = datetime.now()
        # TODO: debug signal handling during execution pool! Unlimited call
        strategies_signals = await asyncio.gather(*run_strategies_tasks)
        try:
            for sig in strategies_signals:
                for s in sig:
                    await self.send_message(f'strategy.signal.{s["strategy"]}', s)
                    print(f"Signalling: {s}")
        except AsyncAppDeliveryError:
            await self.send_status(AppStatus.ERROR, 'No strategy signal listeners running')

    def ampq_bind_funcs(self):
        return {
                'data.quote.*': self.on_new_quote,
        }

    async def main(self):
        log.info("Strategy launcher script is running")


if __name__ == '__main__':
    app = StrategyLauncherScript(app_name='StrategyLauncher')
    app.run()
