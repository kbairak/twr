import datetime
from typing import Generator

market_open = datetime.time(9, 30)
market_close = datetime.time(16, 0)


def is_market_open(ts: datetime.datetime) -> bool:
    # Monday to Friday, market hours
    is_workday = ts.weekday() < 5
    is_market_hours = market_open <= ts.time() <= market_close
    return is_workday and is_market_hours


def get_next_tick(now: datetime.datetime, interval: datetime.timedelta) -> datetime.datetime:
    candidate = now - interval
    while not is_market_open(candidate):
        candidate = datetime.datetime.combine(
            candidate.date() - datetime.timedelta(days=1), market_close
        )
    return candidate


def get_ticks(interval: datetime.timedelta, tick_count: int) -> Generator[datetime.datetime]:
    yield (last := datetime.datetime.now())
    for _ in range(tick_count - 1):
        yield (last := get_next_tick(last, interval))


# duration = interval x tick_count

# Scenario 1: known interval and tick_count
interval, tick_count = datetime.timedelta(minutes=15), 3

result = list(get_ticks(interval, tick_count))
total_duration = result[0] - result[-1]

# Scenario 2: known duration and tick_count

duration, tick_count = datetime.timedelta(days=10), 3
interval = duration / tick_count

result = list(get_ticks(interval, tick_count))
total_duration = result[0] - result[-1]

# Scenario 3: known duration and interval

duration, interval = datetime.timedelta(days=2), datetime.timedelta(hours=18, minutes=30)

tick_count = round(duration / interval)
result = list(get_ticks(interval, tick_count))
total_duration = result[0] - result[-1]
