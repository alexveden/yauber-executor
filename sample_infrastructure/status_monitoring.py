"""
Monitors statuses of all scripts in the systems and sends dummy notifications
"""
from yauber_executor import AsyncApp, AppStatus


class StatusMonitor(AsyncApp):
    async def on_status(self, topic, message):
        print(f'on_status: {topic}  {message}')

        if message['status'] in [AppStatus.ERROR, AppStatus.CRIT, AppStatus.WARN]:
            # TODO: send email notification
            print('Error notification sent')

    def ampq_bind_funcs(self):
        return {
            'status.*': self.on_status,
        }

    async def main(self):
        await self.send_status(AppStatus.IDLE, 'Listening statuses')


if __name__ == '__main__':
    app = StatusMonitor(app_name='StatusMonitor')
    app.run()
