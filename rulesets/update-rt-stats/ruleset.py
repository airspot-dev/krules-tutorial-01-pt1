import os

from krules_core import RuleConst as Const
from krules_core.base_functions import *

from krules_core.event_types import SUBJECT_PROPERTY_CHANGED

import io

rulename = Const.RULENAME
subscribe_to = Const.SUBSCRIBE_TO
ruledata = Const.RULEDATA
filters = Const.FILTERS
processing = Const.PROCESSING

ruleset_config: dict = configs_factory()["rulesets"][os.environ["CE_SOURCE"]]

class UpdateStats(RuleFunctionBase):

    def execute(self):

        import pandas as pd

        prices_history: pd.DataFrame = pd.DataFrame(columns=["price"])
        kline_1m: dict = self.subject.get("kline_1m")
        last_price = float(kline_1m["c"])

        def _atomic_set_ph(cur_v):
            nonlocal prices_history
            if cur_v is not None:
                prices_history = pd.read_pickle(io.BytesIO(eval(cur_v)))
            last_time = pd.Timestamp(kline_1m["E"] * 1000000)
            prices_history.loc[last_time] = last_price
            prices_history = prices_history.sort_index().truncate(
                last_time-pd.Timedelta(**ruleset_config["window_size"])
            )
            buf = io.BytesIO()
            prices_history.to_pickle(buf)

            return str(buf.getvalue())

        self.subject.set("prices_history", _atomic_set_ph, muted=True)

        mean = prices_history["price"].mean()

        diff = (last_price - mean) / mean * 10000

        self.subject.set("rt_stats_raw", {
            "mean": mean,
            "diff": diff,
        }, muted=True)

        direction = float(kline_1m["c"]) >= mean and "up" or "down"

        # reverse speed thresholds dictionary
        thresholds = sorted(ruleset_config.get("speed", []).items(), key=lambda item: item[1], reverse=True)
        if len(thresholds) == 0:
            return
        higher_thsh = thresholds[0][0]

        speed = None
        for thsh in thresholds:
            if abs(diff) >= thsh[1]:
                speed = thsh[0]
                break

        # if still in the higher threshold add a counter
        def _atomic_set_rt_stats_signal(old_value):
            new_value = [speed, direction]
            if old_value is None:
                old_value = new_value
            if new_value[0] == higher_thsh:
                counter = len(old_value) > 2 and old_value[2]+1 or 1
                new_value.append(counter)
            return new_value

        self.subject.set("rt_stats_signal", _atomic_set_rt_stats_signal)


rulesdata = [
    {
        rulename: "on-ticker-set-stats",
        subscribe_to: SUBJECT_PROPERTY_CHANGED,
        ruledata: {
            filters: [
                OnSubjectPropertyChanged("kline_1m"),
            ],
            processing: [
                UpdateStats()
            ]
        }
    },
    {
        rulename: "on-rt_stats_signal-changed-dispatch",
        subscribe_to: SUBJECT_PROPERTY_CHANGED,
        ruledata: {
            filters: [
                OnSubjectPropertyChanged(lambda v: v in ["rt_stats_signal"]),
            ],
            processing: [
                Route(dispatch_policy=DispatchPolicyConst.DIRECT)
            ]
        }
    }
]