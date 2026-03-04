from typing import Any, Callable
from isda_streaming.data_stream import (
    DataStream,
    TimedStream,
    WindowedStream,
    KeyedStream,
    StreamError, 
)
from isda_streaming.synopsis import CountMinSketch, BloomFilter, ReservoirSample 
from pushpushpush.pipelines.utils import (_filter_pkw, _key_by_lane, _get_velocity, _get_max,
                             _filter_lane_1_and_2, _map_lkw_to_count, 
                             _count_lkw_count_all, _cal_percent,
                             _filter_lane_2, _cal_mean_velocity)

from pushpushpush.pipelines.transform_autobahn import pkw_max_velocity_per_lane, lkw_ratio

data_fpath = "data/autobahn.csv"
start = 0
end = 30
input_stream = TimedStream()
input_stream.from_csv(data_fpath, start, end)

result = pkw_max_velocity_per_lane(input_stream)
print(result)