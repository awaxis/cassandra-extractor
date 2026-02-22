import math
from itertools import cycle, islice
from typing import Any, Callable, Optional, Tuple

def smooth(dataset: [Tuple[str, int]]) -> Callable[[], Optional[str]]:
    dataset_length = len(dataset)
    dataset_extra_weights = [ItemWeight(*x) for x in dataset]

    def get_next() -> Optional[str]:
        if dataset_length == 0:
            return None
        if dataset_length == 1:
            return dataset[0][0]

        total_weight = 0
        result = None
        for extra in dataset_extra_weights:
            extra.current_weight += extra.effective_weight
            total_weight += extra.effective_weight
            if extra.effective_weight < extra.weight:
                extra.effective_weight += 1
            if not result or result.current_weight < extra.current_weight:
                result = extra
        if result:
            result.current_weight -= total_weight
            return result.key
        return None

    return get_next


class ItemWeight:
    key: str
    weight: int
    current_weight: int
    effective_weight: int

    def __init__(self, key, weight):
        self.key = key
        self.weight = weight
        self.current_weight = 0
        self.effective_weight = weight

def weighted(dataset: [Tuple[str, int]]) -> Callable[[], Optional[str]]:
    current_index = -1
    current_weight = 0
    dataset_length = len(dataset)
    dataset_max_weight = 0
    dataset_gcd_weight = 0

    for _, weight in dataset:
        if dataset_max_weight < weight:
            dataset_max_weight = weight
        dataset_gcd_weight = math.gcd(dataset_gcd_weight, weight)

    def get_next() -> Optional[str]:
        nonlocal current_index
        nonlocal current_weight
        while True:
            current_index = (current_index + 1) % dataset_length
            if current_index == 0:
                current_weight = current_weight - dataset_gcd_weight
                if current_weight <= 0:
                    current_weight = dataset_max_weight
                    if current_weight == 0:
                        return None
            if dataset[current_index][1] >= current_weight:
                return dataset[current_index][0]

    return get_next

def basic(dataset: [Any]) -> Callable[[], Any]:
    iterator = cycle(dataset)

    def get_next():
        return next(iterator)

    return get_next