from typing import Any, Callable
from isda_streaming.data_stream import (
    DataStream,
    TimedStream,
    WindowedStream,
    KeyedStream,
    StreamError, 
)

from .utils import (_filter_pkw, _key_by_lane, _get_velocity, _get_max,
                             _filter_lane_1_and_2, _map_lkw_to_count, 
                             _count_lkw_count_all, _cal_percent,
                             _filter_lane_2, _cal_mean_velocity)

class TimedStreamNew(TimedStream):
    """
    This new class rewrites some functions that return KeyedStream to return 
    element with timestamp instead of only element.
    """
    def __init__(self, timed_stream: TimedStream| None = None):
        if timed_stream is not None:
            self._stream = timed_stream._stream
            self._timestamps = timed_stream._timestamps
        else:
            super().__init__()

    def key_by(self, key_by_function: Callable[[Any], Any]) -> KeyedStream:
        """Convert the stream into a `KeyedStream` based on a `key_by_function`.

        A `key_by_function` divides a stream into disjoint partitions. All records with
        the same key are assigned to one partition.

        This function adds timestamp to each element in the stream. The timestamp 
        is the same as the timestamp of the original element in the stream.
        """
        result = KeyedStream()
        for i in range(len(self._stream)):
            x = self._stream[i]
            x_timestamp = self._timestamps[i]
            key_x = key_by_function(x)
            if isinstance(key_x, list):
                raise StreamError(
                    "A reduce function must return a single element per processed element."
                )
            result._add_element(key_x, (x, x_timestamp))
        return result

def pkw_max_velocity_per_lane(input_stream: TimedStream) -> KeyedStream:
    """
    die Funktion erstellt eine Dataflow-Pipeline mit folgendem Ergebnis:
    Die laufende maximale Geschwindigkeit eines PKWs pro Autobahnspur
    """
    lkw_stream = TimedStreamNew(input_stream.filter(_filter_pkw))
    return (lkw_stream.key_by(_key_by_lane)
            .map(_get_velocity)
            .reduce(_get_max))

def lkw_ratio(input_stream: TimedStream) -> KeyedStream:
    """
    Erstellt eine Dataflow-Pipeline mit folgendem Ergebnis:
    - Den Anteil der LKWs an allen Fahrzeugen pro Spur lane in Prozent. 
    - Berechnen Sie den Anteil nur für die erste und zweite Spur und 
    runden Sie die Prozentzahl, mit Hilfe der round(Zahl, Nachkommastellen) 
    Funktion von Python, auf zwei Nachkommastellen.
    """
    return (input_stream.filter(_filter_lane_1_and_2)
            .key_by(_key_by_lane)
            .map(_map_lkw_to_count)
            .reduce(_count_lkw_count_all)
            .map(_cal_percent))

def lane_2_min_mean_velocity_100_cars(input_stream: TimedStream) -> Any:
    """
    Erstellt eine Dataflow-Pipeline mit folgendem Ergebnis:
    - Der minimale Wert der durchschnittlichen Geschwindigkeiten (mean_velocity) 
    von 10 Autos auf der mittleren Fahrspur aus den letzten 100 Autos. 
    - Diese Statistik soll alle 50 Autos aktualisiert werden.    
    """
    input_stream = input_stream.filter(_filter_lane_2)
    w1 = input_stream.sliding_tuple_window(100, 50)
    w2 = w1.apply(_cal_mean_velocity)
    return w2.map(lambda x: x[0])