import pytest

from pipelines.transform import pkw_max_velocity_per_lane, lkw_ratio, lane_2_min_mean_velocity_100_cars
# the old solutions got full points, but not very elegant, so we can use them to test our refactored code
import _old_solutions

from isda_streaming.data_stream import KeyedStream, TimedStream

data_fpath = "data/autobahn.csv"

def test_pkw_max_velocity_per_lane():
    start = [0, 100, 300]
    end = [50, 150, 400]
    for s, e in zip(start, end):
        input_stream = TimedStream()
        input_stream.from_csv(data_fpath, s, e)
        assert pkw_max_velocity_per_lane(input_stream) == _old_solutions.pkw_max_velocity_per_lane(input_stream)

def test_pkw_max_velocity_per_lane_empty():
    start = [0, 100, 300]
    end = [50, 150, 400]
    for s, e in zip(start, end):
        input_stream = TimedStream()
        input_stream.from_csv(data_fpath, s, e)
        assert lkw_ratio(input_stream) == _old_solutions.lkw_ratio(input_stream)

def test_lane_2_min_mean_velocity_100_cars():
    start = [0, 100, 300]
    end = [50, 150, 400]
    for s, e in zip(start, end):
        input_stream = TimedStream()
        input_stream.from_csv(data_fpath, s, e)
        assert lane_2_min_mean_velocity_100_cars(input_stream) == _old_solutions.lane_2_min_mean_velocity_100_cars(input_stream)