import datetime


def parse_time(text: str) -> datetime.datetime:
    t = datetime.datetime.strptime(text, "%H:%M")
    return datetime.datetime.now(datetime.timezone.utc).replace(
        hour=t.hour, minute=t.minute, second=0, microsecond=0
    )
