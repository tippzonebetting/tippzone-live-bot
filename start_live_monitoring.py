import asyncio
from live_monitoring_bot import LiveMonitoringBot

if name == “main”:
bot = LiveMonitoringBot()
asyncio.run(bot.start())
