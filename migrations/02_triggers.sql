-- Function to calculate incremental TWR on cash flow insert
--   User provides: user_id, product_id, timestamp, and at least one of (units, money)
--   Optional: outgoing_fees, incoming_fees (default to 0)
--   Trigger calculates: market_price, derives missing units/money, bank_flow,
--                      cumulative_units, cumulative_money, bank account totals,
--                      period_return, cumulative_twr_factor
CREATE OR REPLACE FUNCTION calculate_incremental_twr()
RETURNS TRIGGER AS $$
DECLARE
    v_market_price NUMERIC;
    v_prev_price NUMERIC;
    v_prev_timestamp TIMESTAMPTZ;
    v_prev_cumulative_twr_factor NUMERIC;
    v_prev_cumulative_units NUMERIC;
    v_prev_cumulative_money NUMERIC;
    v_prev_cumulative_bank_flow NUMERIC;
    v_prev_total_deposits NUMERIC;
    v_prev_total_withdrawals NUMERIC;
    v_prev_cumulative_outgoing_fees NUMERIC;
    v_prev_cumulative_incoming_fees NUMERIC;
    v_prev_value_after_flow NUMERIC;
    v_value_before_flow NUMERIC;
BEGIN
    -- Ensure timestamp is in UTC
    NEW.timestamp := timezone('UTC', NEW.timestamp);

    -- Get market price for this product (most recent price <= transaction timestamp)
    SELECT price INTO v_market_price
    FROM product_price
    WHERE product_id = NEW.product_id AND timestamp <= NEW.timestamp
    ORDER BY timestamp DESC
    LIMIT 1;

    IF v_market_price IS NULL THEN
        RAISE EXCEPTION 'No price found for product_id % at or before timestamp %',
                        NEW.product_id, NEW.timestamp;
    END IF;

    -- Store the market price
    NEW.market_price := v_market_price;

    -- Derive missing field if needed (user must provide at least one)
    IF NEW.units IS NULL AND NEW.money IS NULL THEN
        RAISE EXCEPTION 'Must provide at least one of: units or money';
    END IF;

    IF NEW.units IS NULL THEN
        -- User provided money, calculate units
        NEW.units := NEW.money / v_market_price;
    ELSIF NEW.money IS NULL THEN
        -- User provided units, calculate money
        NEW.money := NEW.units * v_market_price;
    END IF;
    -- If both provided, use both as-is (captures slippage/spread)

    -- Ensure fees are not NULL
    IF NEW.outgoing_fees IS NULL THEN
        NEW.outgoing_fees := 0;
    END IF;
    IF NEW.incoming_fees IS NULL THEN
        NEW.incoming_fees := 0;
    END IF;

    -- Calculate bank flow
    -- For buys (money > 0): bank_flow = -(money + outgoing_fees) [money leaving bank]
    -- For sells (money < 0): bank_flow = -(money - incoming_fees) = -money + incoming_fees [money entering bank]
    IF NEW.money >= 0 THEN
        NEW.bank_flow := -(NEW.money + NEW.outgoing_fees);
    ELSE
        NEW.bank_flow := -(NEW.money - NEW.incoming_fees);
    END IF;

    -- Get previous cash flow for this user-product pair
    SELECT cumulative_twr_factor, cumulative_units, cumulative_money,
           cumulative_bank_flow, total_deposits, total_withdrawals,
           cumulative_outgoing_fees, cumulative_incoming_fees, timestamp
    INTO v_prev_cumulative_twr_factor,
         v_prev_cumulative_units,
         v_prev_cumulative_money,
         v_prev_cumulative_bank_flow,
         v_prev_total_deposits,
         v_prev_total_withdrawals,
         v_prev_cumulative_outgoing_fees,
         v_prev_cumulative_incoming_fees,
         v_prev_timestamp
    FROM cash_flow
    WHERE user_id = NEW.user_id AND
          product_id = NEW.product_id AND
          timestamp < NEW.timestamp
    ORDER BY timestamp DESC
    LIMIT 1;

    -- If there was a previous flow, get the market price at that time
    IF v_prev_cumulative_units IS NOT NULL THEN
        SELECT price INTO v_prev_price
        FROM product_price
        WHERE product_id = NEW.product_id
          AND timestamp <= v_prev_timestamp
        ORDER BY timestamp DESC
        LIMIT 1;
    END IF;

    -- Calculate cumulative values and TWR
    IF v_prev_cumulative_units IS NULL THEN
        -- First transaction for this user-product
        NEW.cumulative_units := NEW.units;
        NEW.cumulative_money := NEW.money;
        NEW.cumulative_bank_flow := NEW.bank_flow;
        NEW.cumulative_outgoing_fees := NEW.outgoing_fees;
        NEW.cumulative_incoming_fees := NEW.incoming_fees;

        -- Initialize bank account totals
        IF NEW.bank_flow < 0 THEN
            -- Money leaving bank (deposit to investment)
            NEW.total_deposits := ABS(NEW.bank_flow);
            NEW.total_withdrawals := 0;
        ELSE
            -- Money entering bank (withdrawal from investment)
            NEW.total_deposits := 0;
            NEW.total_withdrawals := NEW.bank_flow;
        END IF;

        NEW.period_return := 0;
        NEW.cumulative_twr_factor := 1.0;
    ELSE
        -- Calculate cumulative values incrementally
        NEW.cumulative_units := v_prev_cumulative_units + NEW.units;
        NEW.cumulative_money := v_prev_cumulative_money + NEW.money;
        NEW.cumulative_bank_flow := v_prev_cumulative_bank_flow + NEW.bank_flow;
        NEW.cumulative_outgoing_fees := v_prev_cumulative_outgoing_fees + NEW.outgoing_fees;
        NEW.cumulative_incoming_fees := v_prev_cumulative_incoming_fees + NEW.incoming_fees;

        -- Update bank account totals
        IF NEW.bank_flow < 0 THEN
            -- Money leaving bank (deposit to investment)
            NEW.total_deposits := v_prev_total_deposits + ABS(NEW.bank_flow);
            NEW.total_withdrawals := v_prev_total_withdrawals;
        ELSE
            -- Money entering bank (withdrawal from investment)
            NEW.total_deposits := v_prev_total_deposits;
            NEW.total_withdrawals := v_prev_total_withdrawals + NEW.bank_flow;
        END IF;

        -- Calculate value after previous flow using market price at that time
        v_prev_value_after_flow := v_prev_cumulative_units * v_prev_price;

        -- Calculate current value before the cash flow using current market price
        v_value_before_flow := v_prev_cumulative_units * v_market_price;

        -- Calculate period return
        -- period_return = (value_before_flow - prev_value_after_flow) / prev_value_after_flow
        IF v_prev_value_after_flow = 0 THEN
            -- If previous value was zero (sold everything), return is undefined
            -- Set to 0 for this period
            NEW.period_return := 0;
            NEW.cumulative_twr_factor := v_prev_cumulative_twr_factor;
        ELSE
            NEW.period_return := (v_value_before_flow - v_prev_value_after_flow) / v_prev_value_after_flow;
            -- Compound the TWR: new_factor = prev_factor * (1 + period_return)
            NEW.cumulative_twr_factor := v_prev_cumulative_twr_factor * (1 + NEW.period_return);
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to calculate TWR on insert
CREATE TRIGGER calculate_twr_trigger
  BEFORE INSERT ON cash_flow
  FOR EACH ROW
  EXECUTE FUNCTION calculate_incremental_twr();
