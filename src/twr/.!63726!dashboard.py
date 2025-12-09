"""Streamlit dashboard for product price generation and visualization"""

import streamlit as st
from datetime import datetime, time, timedelta
import plotly.express as px
import psycopg2

from twr.dashboard_utils import (
    parse_frequency,
    generate_trading_timestamps,
    generate_prices_linear_interpolation,
    insert_product_and_prices,
    load_all_products,
    load_price_data,
)

st.set_page_config(
    page_title="Product Price Generator",
