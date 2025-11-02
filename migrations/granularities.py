"""
Granularity configurations for multi-tier bucketing system.

Each granularity defines:
- suffix: Identifier used in table/view names (e.g., _15min, _1h, _1d)
- interval: TimescaleDB time_bucket interval
- cache_retention: How long to keep cache entries (None = forever)
- include_realtime: Whether to include Tier 3 (raw prices after last bucket)
- use_case: When to use this granularity
- description: Brief explanation
"""

GRANULARITIES = [
    {
        'suffix': '15min',
        'interval': '15 minutes',
        'cache_retention': '7 days',
        'include_realtime': True,  # Enable Tier 3 for real-time freshness
        'use_case': 'Real-time monitoring and recent detailed analysis',
        'description': 'High precision with real-time data for active trading periods (last 7 days)',
    },
    {
        'suffix': '1h',
        'interval': '1 hour',
        'cache_retention': '30 days',
        'include_realtime': False,  # Bucketed-only, no real-time tier
        'use_case': 'Weekly and monthly performance analysis',
        'description': 'Balanced precision for recent historical analysis (last 30 days)',
    },
    {
        'suffix': '1d',
        'interval': '1 day',
        'cache_retention': None,  # Keep cache indefinitely
        'include_realtime': False,  # Bucketed-only, no real-time tier
        'use_case': 'Long-term trends and multi-year analysis',
        'description': 'Daily precision for historical data (retained indefinitely)',
    },
]
