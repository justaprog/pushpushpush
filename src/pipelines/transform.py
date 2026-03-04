from typing import Any
from isda_streaming.data_stream import (
    DataStream,
    TimedStream,
    WindowedStream,
    KeyedStream,
)
from isda_streaming.synopsis import CountMinSketch, BloomFilter, ReservoirSample 
from pipelines.utils import (_filter_pkw, _key_by_lane, _get_velocity, _get_max,
                             _filter_lane_1_and_2, _map_lkw_to_count, 
                             _count_lkw_count_all, _cal_percent,
                             _filter_lane_2, _cal_mean_velocity)

def pkw_max_velocity_per_lane(input_stream: TimedStream) -> KeyedStream:
    """
    die Funktion erstellt eine Dataflow-Pipeline mit folgendem Ergebnis:
    Die laufende maximale Geschwindigkeit eines PKWs pro Autobahnspur
    """
    return (input_stream.filter(_filter_pkw)
            .key_by(_key_by_lane)
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