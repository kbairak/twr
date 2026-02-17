"""Streamlit dashboard for product price generation and visualization"""

from datetime import datetime, time, timedelta

import plotly.express as px
import psycopg2
import streamlit as st
from dashboard_utils import (
    create_user,
    generate_prices_linear_interpolation,
    generate_trading_timestamps,
    insert_cashflow,
    insert_product_and_prices,
    load_all_products,
    load_all_users,
    load_price_data,
    load_user_product_timeline,
    parse_frequency,
)

st.set_page_config(
    page_title="Product Price Generator",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Product Price Generator & Visualizer")

# Create tabs for different functionalities
tab1, tab2 = st.tabs(["📈 Generate Prices", "💰 Insert Cashflow"])

# Tab 1: Price Generation
with tab1:
    st.subheader("Generate Product Prices")

    with st.form("price_generator"):
        col1, col2 = st.columns(2)

        with col1:
            name = st.text_input("Product Name", placeholder="e.g., AAPL, MSFT")
            start_price = st.number_input(
                "Start Price ($)", min_value=0.01, value=100.0, step=0.01
            )
            start_date = st.date_input("Start Date", value=datetime.now().date())
            start_time_input = st.time_input("Start Time", value=time(9, 30))

        with col2:
            end_price = st.number_input("End Price ($)", min_value=0.01, value=150.0, step=0.01)
            default_end_date = datetime.now().date() + timedelta(days=7)
            end_date = st.date_input("End Date", value=default_end_date)
            end_time_input = st.time_input("End Time", value=time(16, 0))
            frequency = st.selectbox(
                "Update Frequency",
                ["1 min", "2 min", "5 min", "30 min", "1 hour", "2 hours", "daily"],
                index=3,  # Default to 30 min
            )

        submit = st.form_submit_button("Generate & Insert Prices", type="primary")

    # Handle form submission
    if submit:
        errors = []

        # Validate product name
        if not name or not name.strip():
            errors.append("Product name is required")

        # Validate dates
        start_dt = datetime.combine(start_date, start_time_input)
        end_dt = datetime.combine(end_date, end_time_input)

        if end_dt <= start_dt:
            errors.append("End date/time must be after start date/time")

        # Validate prices
        if start_price <= 0:
            errors.append("Start price must be positive")
        if end_price <= 0:
            errors.append("End price must be positive")

        # Show errors if any
        if errors:
            for error in errors:
                st.error(error)
        else:
            # Generate timestamps and prices
            try:
                with st.spinner("Generating timestamps..."):
                    freq_delta = parse_frequency(frequency)
                    timestamps = generate_trading_timestamps(start_dt, end_dt, freq_delta)

                # Check if too many timestamps
                if len(timestamps) > 10000:
                    st.warning(
                        f"This will generate {len(timestamps):,} price points. This may take a while."
                    )

                with st.spinner("Generating prices..."):
                    prices = generate_prices_linear_interpolation(
                        start_price, end_price, timestamps
                    )

                # Create list of (timestamp, price) tuples
                timestamps_and_prices = list(zip(timestamps, prices))

                # Insert to database
                with st.spinner("Inserting into database..."):
                    insert_product_and_prices(name.strip(), timestamps_and_prices)

                st.success(
                    f"✓ Successfully generated {len(timestamps_and_prices):,} price points for **{name}**"
                )
                st.info(f"Price range: ${min(prices):.2f} - ${max(prices):.2f}")

                # Force rerun to update the chart
                st.rerun()

            except ValueError as e:
                st.error(f"Invalid input: {e}")
            except psycopg2.OperationalError as e:
                st.error(f"Database connection failed: {e}")
                st.info("💡 Make sure PostgreSQL is running: `docker compose up -d`")
            except Exception as e:
                st.error(f"Error generating prices: {e}")
                with st.expander("Show full error"):
                    st.exception(e)

# Tab 2: Cashflow Insertion
with tab2:
    st.subheader("Insert Cashflow Transaction")

    # Load users and products for dropdowns
    try:
        users = load_all_users()
        products_for_cashflow = load_all_products()

        if not products_for_cashflow:
            st.warning("No products found. Please generate some prices first in the other tab.")
        else:
            # User selection/creation
            col_user, col_new_user = st.columns([3, 1])
            with col_user:
                if users:
                    user_names = [u["name"] for u in users]
                    selected_user_name = st.selectbox("Select User", user_names)
                    selected_user_id = next(
                        u["id"] for u in users if u["name"] == selected_user_name
                    )
                else:
                    st.info("No users found. Create one below.")
                    selected_user_id = None
                    selected_user_name = None

            with col_new_user:
                new_user_name = st.text_input("Or create new user", placeholder="John Doe")
                if st.button("Create User") and new_user_name:
                    try:
                        new_user_id = create_user(new_user_name.strip())
                        st.success(f"✓ Created user: {new_user_name}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error creating user: {e}")

            # Only show cashflow form if we have a user
            if selected_user_id or new_user_name:
                st.divider()

                # Product selection
                product_names_cf = [p["name"] for p in products_for_cashflow]
                selected_product_name = st.selectbox("Select Product", product_names_cf)
                selected_product_id = next(
                    p["id"] for p in products_for_cashflow if p["name"] == selected_product_name
                )

                # Load price data for the selected product to populate the slider
                price_df = load_price_data([selected_product_id])

                if price_df.empty:
                    st.warning(
                        f"No price data found for {selected_product_name}. Generate prices first."
                    )
                else:
                    # Time slider
                    min_time = price_df["timestamp"].min().to_pydatetime()
                    max_time = price_df["timestamp"].max().to_pydatetime()

                    # Store selected timestamp in session state for persistence
                    if "selected_cashflow_time" not in st.session_state:
                        st.session_state.selected_cashflow_time = min_time

                    selected_timestamp = st.slider(
                        "Transaction Time",
                        min_value=min_time,
                        max_value=max_time,
                        value=st.session_state.selected_cashflow_time,
                        format="YYYY-MM-DD HH:mm",
                        help="Slide to select the exact time of the transaction",
                    )
                    st.session_state.selected_cashflow_time = selected_timestamp

                    # Find and display the price at selected timestamp
                    closest_idx = (price_df["timestamp"] - selected_timestamp).abs().idxmin()
                    market_price = price_df.loc[closest_idx, "price"]
                    actual_time = price_df.loc[closest_idx, "timestamp"]

                    st.info(
                        f"📊 Market price at {actual_time.strftime('%Y-%m-%d %H:%M:%S')}: **${market_price:.2f}**"
                    )

                    # Cashflow form
                    with st.form("cashflow_form"):
                        st.write("**Transaction Details**")

                        col1, col2 = st.columns(2)

                        with col1:
                            units_delta = st.number_input(
                                "Units",
                                value=10.0,
                                step=0.01,
                                help="Number of units to buy (positive) or sell (negative)",
                            )
                            execution_price = st.number_input(
                                "Price per Unit ($)",
                                value=float(market_price),
                                step=0.01,
                                help="Execution price (defaults to market price)",
                            )

                        with col2:
                            fees = st.number_input(
                                "Fees ($)",
                                value=0.0,
                                min_value=0.0,
                                step=0.01,
                                help="Transaction fees",
                            )

                        submit_cashflow = st.form_submit_button("Insert Cashflow", type="primary")

                    if submit_cashflow:
                        try:
                            # Use the user we selected or just created
                            user_id_to_use = selected_user_id if selected_user_id else new_user_id

                            insert_cashflow(
                                user_id=user_id_to_use,
                                product_id=selected_product_id,
                                timestamp=selected_timestamp,
                                units_delta=units_delta,
                                execution_price=execution_price,
                                fees=fees,
                            )
                            st.success("✓ Cashflow inserted successfully!")

                            # Calculate what was inserted
                            execution_money = units_delta * execution_price
                            user_money = execution_money + fees
                            st.info(
                                f"User: **{selected_user_name}** | Product: **{selected_product_name}** | Time: {selected_timestamp.strftime('%Y-%m-%d %H:%M')}"
                            )
                            st.info(
                                f"Total cost: ${abs(user_money):.2f} (${abs(execution_money):.2f} + ${fees:.2f} fees)"
                            )

                        except Exception as e:
                            st.error(f"Error inserting cashflow: {e}")
                            with st.expander("Show full error"):
                                st.exception(e)

    except Exception as e:
        st.error(f"Error loading data: {e}")
        with st.expander("Show full error"):
            st.exception(e)

st.divider()

# Section 2: Charts with tabs
st.subheader("Charts")

# Create tabs for different chart views
chart_tab1, chart_tab2 = st.tabs(["📈 Product Prices", "👤 User Portfolio Timeline"])

# Tab 1: Product Prices
with chart_tab1:
    try:
        # Load all products
        products = load_all_products()

        if not products:
            st.warning("No products found. Generate some prices using the form above.")
        else:
            # Product selection
            product_names = [p["name"] for p in products]
            selected_names = st.multiselect(
                "Select Products to Display",
                product_names,
                default=product_names[: min(5, len(product_names))],  # Default to first 5
                help="Select one or more products to compare their prices",
            )

            if selected_names:
                # Get product IDs for selected names
                selected_ids = [p["id"] for p in products if p["name"] in selected_names]

                # Load price data
                with st.spinner("Loading price data..."):
                    df = load_price_data(selected_ids)

                if df.empty:
                    st.info("No price data found for selected products.")
                else:
                    # Create Plotly chart
                    fig = px.line(
                        df,
                        x="timestamp",
                        y="price",
                        color="product_name",
                        title="Product Price Comparison",
                        labels={
                            "timestamp": "Time",
                            "price": "Price ($)",
                            "product_name": "Product",
                        },
                    )

                    # Add vertical line at selected cashflow time (if one is selected)
                    if "selected_cashflow_time" in st.session_state:
                        fig.add_vline(
                            x=st.session_state.selected_cashflow_time,
                            line_dash="dash",
                            line_color="red",
                            line_width=2,
                        )

                    # Customize chart
                    fig.update_layout(
                        hovermode="x unified",
                        legend=dict(
                            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                        ),
                    )

                    st.plotly_chart(fig, use_container_width=True)

                    # Show summary statistics
                    with st.expander("📊 Summary Statistics"):
                        col1, col2, col3 = st.columns(3)

                        with col1:
                            st.metric("Total Data Points", f"{len(df):,}")

                        with col2:
                            st.metric(
                                "Date Range",
                                f"{df['timestamp'].min().strftime('%Y-%m-%d')} to {df['timestamp'].max().strftime('%Y-%m-%d')}",
                            )

                        with col3:
                            st.metric("Products Selected", len(selected_names))

            else:
                st.info("Select at least one product to view the chart.")

    except psycopg2.OperationalError:
        st.error("Unable to connect to the database.")
        st.info("💡 Make sure PostgreSQL is running: `docker compose up -d`")
    except Exception as e:
        st.error(f"Error loading products: {e}")
        with st.expander("Show full error"):
            st.exception(e)

# Tab 2: User Portfolio Timeline
with chart_tab2:
    try:
        # Load all users
        users_for_timeline = load_all_users()

        if not users_for_timeline:
            st.warning("No users found. Create a user and insert some cashflows first.")
        else:
            # User selection
            user_names_timeline = [u["name"] for u in users_for_timeline]
            selected_user_names = st.multiselect(
                "Select Users to Display",
                user_names_timeline,
                default=user_names_timeline[: min(3, len(user_names_timeline))],
                help="Select one or more users to view their portfolio timeline",
            )

            if selected_user_names:
                # Get user IDs for selected names
                selected_user_ids = [
                    u["id"] for u in users_for_timeline if u["name"] in selected_user_names
                ]

                # Load timeline data
                with st.spinner("Loading portfolio timeline..."):
                    timeline_df = load_user_product_timeline(selected_user_ids)

                if timeline_df.empty:
                    st.info("No timeline data found. Insert some cashflows first.")
                else:
                    # Create a combined identifier for grouping
                    timeline_df["user_product"] = (
                        timeline_df["user_name"] + " - " + timeline_df["product_name"]
                    )

                    # Create Plotly chart for market value
                    fig = px.line(
                        timeline_df,
                        x="timestamp",
                        y="market_value",
                        color="user_product",
                        title="Portfolio Value Over Time",
                        labels={
                            "timestamp": "Time",
                            "market_value": "Market Value ($)",
                            "user_product": "User - Product",
                        },
                    )

                    # Add vertical line at selected cashflow time (if one is selected)
                    if "selected_cashflow_time" in st.session_state:
                        fig.add_vline(
                            x=st.session_state.selected_cashflow_time,
                            line_dash="dash",
                            line_color="red",
                            line_width=2,
                        )

                    # Customize chart
                    fig.update_layout(
                        hovermode="x unified",
                        legend=dict(
                            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                        ),
                    )

                    st.plotly_chart(fig, use_container_width=True)

                    # Show summary statistics
                    with st.expander("📊 Timeline Statistics"):
                        col1, col2, col3 = st.columns(3)

                        with col1:
                            st.metric("Total Data Points", f"{len(timeline_df):,}")

                        with col2:
                            st.metric(
                                "Date Range",
                                f"{timeline_df['timestamp'].min().strftime('%Y-%m-%d')} to {timeline_df['timestamp'].max().strftime('%Y-%m-%d')}",
                            )

                        with col3:
                            total_value = (
                                timeline_df.groupby("timestamp")["market_value"].sum().iloc[-1]
                                if len(timeline_df) > 0
                                else 0
                            )
                            st.metric("Latest Total Value", f"${total_value:,.2f}")

            else:
                st.info("Select at least one user to view the timeline.")

    except psycopg2.OperationalError:
        st.error("Unable to connect to the database.")
        st.info("💡 Make sure PostgreSQL is running: `docker compose up -d`")
    except Exception as e:
        st.error(f"Error loading timeline: {e}")
        with st.expander("Show full error"):
            st.exception(e)
