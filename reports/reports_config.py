
import numpy as np
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Optional

PriceHistory = namedtuple(
    typename="PriceHistory", field_names=["history_index", "history_price"]
)
PriceHistory.__annotations__ = {"history_index": int, "history_price": float}


def sum_of_differences(values):
    if values is None or len(values) <= 3:
        return 1
    mean = sum(values) / len(values)
    return sum(abs(value - mean) for value in values)


def abbe_criterion(signal):
    """Критерий Аббе."""
    if signal is None or len(signal) <= 3:
        return 0
    differences = np.diff(signal) ** 2
    squares = np.sum(differences)
    return np.sqrt(squares / (len(signal) - 1))


@dataclass
class Material:
    code: str
    name: str
    base_price: float
    index_num: int
    actual_price: float
    monitoring_index: int
    monitoring_price: float
    transport_flag: bool = False
    transport_code: str = ""
    transport_base_price: float = 0.0
    transport_factor: float = 0.0
    gross_weight: float = 0.0
    history: list[PriceHistory] = field(default_factory=list)
    len_history: int = 0
    abbe_criterion: float = 0.0
    mean_history: float = 0.0
    std_history: float = 0.0

    def __post_init__(self):
        if self.history is None:
            raise ValueError("history is None")
        self.len_history = len(self.history)
        self.abbe_criterion = abbe_criterion([x.history_price for x in self.history])
        self.mean_history = np.mean([x.history_price for x in self.history])
        self.std_history = np.std([x.history_price for x in self.history])
