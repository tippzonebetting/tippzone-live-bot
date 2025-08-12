import asyncio
from live_monitoring_bot import LiveMonitoringBot

if __name__ == "__main__":
    bot = LiveMonitoringBot()
    asyncio.run(bot.start())
