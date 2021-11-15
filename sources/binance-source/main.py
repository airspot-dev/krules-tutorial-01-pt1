import os
import krules_env
krules_env.init()

from krules_core.providers import (
    subject_factory,
    configs_factory,
)

service_config: dict = configs_factory()["services"][os.environ["CE_SOURCE"]]

from binance import ThreadedWebsocketManager

def _callback(msg):
    # https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
    if msg["e"] == "kline":
        msg["k"]["E"] = msg["E"]  # event time
        subject_factory(f"symbol:{msg['s'].lower()}") \
            .set(f'kline_{msg["k"]["i"]}', msg["k"], use_cache=False)  # kline_1m


twm = ThreadedWebsocketManager(api_key='', api_secret='')
twm.start()

for stream in service_config["streams"]:
    twm.start_kline_socket(_callback, stream, "1m")

twm.join()