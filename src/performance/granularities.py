from dataclasses import dataclass


@dataclass
class Granularity:
    suffix: str
    interval: str
    cache_retention: str | None
    include_realtime: bool


GRANULARITIES = (
    Granularity("15min", "15 minutes", "7 days", True),
    Granularity("1h", "1 hour", "30 days", False),
    Granularity("1d", "1 day", None, False),
)
