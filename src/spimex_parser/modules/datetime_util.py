import datetime
from typing import Generator

def datetime_range(
    start: datetime.datetime,
    end: datetime.datetime,
) -> Generator[datetime.datetime, None, None]:
    """Генерирует последовательные даты в указанном диапазоне"""
    current_date = start

    while current_date < end:
        yield current_date
        current_date += datetime.timedelta(days=1)
