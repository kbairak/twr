"""TWR (Time-Weighted Return) Calculator

A PostgreSQL-based portfolio tracking system with incremental caching.
"""

from .database import TWRDatabase
from .event_generator import EventGenerator
from .benchmark import Benchmark

__version__ = "1.0.0"

__all__ = ["TWRDatabase", "EventGenerator", "Benchmark"]
