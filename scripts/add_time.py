from typing import Any, Callable
from isda_streaming.data_stream import (
    DataStream,
    TimedStream,
    WindowedStream,
    KeyedStream,
    StreamError, 
)

from pushpushpush.pipelines.transform_autobahn import pkw_max_velocity_per_lane, lkw_ratio

data_fpath = "data/autobahn.csv"
start = 0
end = 30
input_stream = TimedStream()
input_stream.from_csv(data_fpath, start, end)

result = pkw_max_velocity_per_lane(input_stream)
print(result)