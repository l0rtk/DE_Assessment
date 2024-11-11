from datetime import datetime
from typing import List
import numpy as np

def calculate_shannon_diversity(values: List[int]) -> float:
    if not values:
        return 0
    total = sum(values)
    proportions = [n/total for n in values]
    return -sum(p * np.log(p) for p in proportions if p > 0)

def parse_date(date_str: str) -> datetime:
    """return date string in DD.MM.YYYY format"""
    return datetime.strptime(date_str, '%d.%m.%Y')
