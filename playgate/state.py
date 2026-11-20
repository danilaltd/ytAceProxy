import asyncio

class AppContext:
    def __init__(self):
        self.stop_event = asyncio.Event()

appContext = AppContext()