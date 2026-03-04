from typing import Any
from isda_streaming.data_stream import (
    DataStream,
    TimedStream,
    WindowedStream,
    KeyedStream,
)
from isda_streaming.synopsis import CountMinSketch, BloomFilter, ReservoirSample 

# Implementieren Sie die Funktion pkw_max_velocity_per_lane, die eine Dataflow-Pipeline mit folgendem Ergebnis erstellt:
# Die laufende maximale Geschwindigkeit eines PKWs pro Autobahnspur.
def _filter_pkw(stream_element: tuple) -> bool:
    if stream_element[2] == 'pkw':
        return True
    return False

def _key_by_lane(stream_element: tuple) -> int:
    return stream_element[0]

def _get_velocity(stream_element: tuple) -> float:
    return stream_element[1]

def _get_max(velocity_1: float, velocity_2: float) -> float:
    if velocity_1 > velocity_2:
        return velocity_1
    return velocity_2


def pkw_max_velocity_per_lane(input_stream: TimedStream) -> Any:
    return input_stream.filter(_filter_pkw).key_by(_key_by_lane).map(_get_velocity).reduce(_get_max)


