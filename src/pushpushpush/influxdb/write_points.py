# https://docs.influxdata.com/influxdb3/core/write-data/client-libraries/?t=Python
from email.policy import default
import os
from dotenv import load_dotenv

from influxdb_client_3 import (
  InfluxDBClient3, InfluxDBError, Point, WritePrecision,
  WriteOptions, write_client_options)

load_dotenv()
host = "http://localhost:8181/api/v3/configure/database" # can be set via environment variable INFLUX_HOST
token = os.getenv('INFLUXDB3_AUTH_TOKEN')
#database = "testdb" # can be set via environment variable INFLUX_DATABASE, default to "testdb"

# Create an array of points with tags and fields.
#points = [Point("home")
#            .tag("room", "Kitchen")
#            .field("temp", 25.3)
#            .field('hum', 20.2)
#            .field('co', 9)]

# With batching mode, define callbacks to execute after a successful or
# failed write request.
# Callback methods receive the configuration and data sent in the request.
def success(self, data: str):
    print(f"Successfully wrote batch: data: {data}")

def error(self, data: str, exception: InfluxDBError):
    print(f"Failed writing batch: config: {self}, data: {data} due: {exception}")

def retry(self, data: str, exception: InfluxDBError):
    print(f"Failed retry writing batch: config: {self}, data: {data} retry: {exception}")

def write_points_to_influxdb(points: list[Point], batch_size: int = 500, database: str = "testdb") -> None:
    """
    Write points to InfluxDB with batching.
    """
    # Configure options for batch writing.
    write_options = WriteOptions(batch_size=batch_size,
                                        flush_interval=10_000,
                                        jitter_interval=2_000,
                                        retry_interval=5_000,
                                        max_retries=5,
                                        max_retry_delay=30_000,
                                        exponential_base=2)

    # Create an options dict that sets callbacks and WriteOptions.
    wco = write_client_options(success_callback=success,
                            error_callback=error,
                            retry_callback=retry,
                            write_options=write_options)

    # Instantiate a synchronous instance of the client with your
    # InfluxDB credentials and write options, such as Gzip threshold, default tags,
    # and timestamp precision. Default precision is nanosecond ('ns').
    with InfluxDBClient3(host=host,
                            token=token,
                            database=database,
                            write_client_options=wco) as client:

        client.write(points, write_precision='s')
