from isda_streaming.data_stream import (
    DataStream,
    TimedStream,
    WindowedStream,
    KeyedStream,
    StreamError, 
)

from influxdb_client_3 import Point, WritePrecision


from pushpushpush.pipelines.transform_autobahn import pkw_max_velocity_per_lane, lkw_ratio
from pushpushpush.influxdb.write_points import write_points_to_influxdb

data_fpath = "data/autobahn.csv"
input_stream = TimedStream()
input_stream.from_csv(data_fpath)

result = pkw_max_velocity_per_lane(input_stream)

for lane, list_result in result._streams.items():
    list_to_add = []
    for velocity, timestamp in list_result:
        # print(f"Lane: {lane}, Max Velocity: {velocity}, Timestamp: {str(int(timestamp))}")
        # Lane: 2.0, Max Velocity: 124.0, Timestamp: 1590969601
        list_to_add.append(Point("pkw_max_velocity_per_lane") # measurement name
            .tag("lane", str(lane))
            .field("max_velocity", float(velocity)) 
            .time(int(timestamp), write_precision=WritePrecision.S))
    if len(list_to_add) > 0:
        write_points_to_influxdb(list_to_add, batch_size=len(list_to_add), database="autobahn")

print("Done writing points to InfluxDB.")