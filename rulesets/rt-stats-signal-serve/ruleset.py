from krules_core import RuleConst as Const
from krules_core.base_functions import *
from krules_core.event_types import SUBJECT_PROPERTY_CHANGED
import websockets
import asyncio
import threading


rulename = Const.RULENAME
subscribe_to = Const.SUBSCRIBE_TO
ruledata = Const.RULEDATA
filters = Const.FILTERS
processing = Const.PROCESSING

CLIENTS = set()


async def handler(websocket, path):
    CLIENTS.add(websocket)
    try:
        async for _ in websocket:
            pass
    finally:
        CLIENTS.remove(websocket)


async def start_server():
    async with websockets.serve(handler, "localhost", 8850):
        await asyncio.Future()


def loop_in_thread(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_server())


loop = asyncio.get_event_loop()
t = threading.Thread(target=loop_in_thread, args=(loop,))
t.start()


class SendMessage(RuleFunctionBase):

    async def _send(self, ws):
        values = self.payload["value"]
        counter = len(values) > 2 and f"[{values[2]}]" or ""
        msg = f"{self.subject.name.split(':')[1]} is going {values[1]} " \
              f"{values[0]}{counter} ({self.subject.get('rt_stats_raw')['diff']})"
        await ws.send(msg)

    def execute(self):
        for ws in CLIENTS:
            asyncio.run(self._send(ws))


rulesdata = [
    {
        rulename: "on-rt-stats-signal-ws-send",
        subscribe_to: SUBJECT_PROPERTY_CHANGED,
        ruledata: {
            processing: [
                SendMessage()
            ]
        }
    },
]
