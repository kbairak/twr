-- Function to calculate incremental TWR on cash flow insert
--   Provide the new row with: user_id, product_id, timestamp, units
--   Enhances NEW with: deposit, cumulative_units, cumulative_deposits,
--                      period_return, cumulative_twr_factor
CREATE OR REPLACE FUNCTION calculate_incremental_twr()
RETURNS TRIGGER AS $$
DECLARE
    v_current_price NUMERIC;
    v_prev_price NUMERIC;
    v_prev_timestamp TIMESTAMPTZ;
    v_prev_cumulative_twr_factor NUMERIC;
    v_prev_cumulative_units NUMERIC;
    v_prev_cumulative_deposits NUMERIC;
    v_prev_value_after_flow NUMERIC;
    v_value_before_flow NUMERIC;
BEGIN
    -- Ensure timestamp is in UTC
    NEW.timestamp := timezone('UTC', NEW.timestamp);

    -- Get current price for this product (most recent price <= transaction timestamp)
    SELECT price INTO v_current_price
    FROM product_price
    WHERE product_id = NEW.product_id AND timestamp <= NEW.timestamp
    ORDER BY timestamp DESC
    LIMIT 1;

    IF v_current_price IS NULL THEN
        RAISE EXCEPTION 'No price found for product_id % at or before timestamp %',
                        NEW.product_id, NEW.timestamp;
    END IF;

    -- Get previous cash flow for this user-product pair (O(1) lookup)
    SELECT
        cumulative_twr_factor,
        cumulative_units,
        cumulative_deposits,
        timestamp
    INTO
        v_prev_cumulative_twr_factor,
        v_prev_cumulative_units,
        v_prev_cumulative_deposits,
        v_prev_timestamp
    FROM user_cash_flow
    WHERE user_id = NEW.user_id
      AND product_id = NEW.product_id
      AND timestamp < NEW.timestamp
    ORDER BY timestamp DESC
    LIMIT 1;

    -- If there was a previous flow, get the price at that time
    IF v_prev_cumulative_units IS NOT NULL THEN
        SELECT price INTO v_prev_price
        FROM product_price
        WHERE product_id = NEW.product_id
          AND timestamp <= v_prev_timestamp
        ORDER BY timestamp DESC
        LIMIT 1;
    END IF;

    -- Calculate deposit for this transaction
    NEW.deposit := NEW.units * v_current_price;

    -- Calculate cumulative values and TWR
    IF v_prev_cumulative_units IS NULL THEN
        -- First transaction for this user-product
        NEW.cumulative_units := NEW.units;
        NEW.cumulative_deposits := NEW.deposit;
        NEW.period_return := 0;
        NEW.cumulative_twr_factor := 1.0;
    ELSE
        -- Calculate cumulative units and deposits incrementally
        NEW.cumulative_units := v_prev_cumulative_units + NEW.units;
        NEW.cumulative_deposits := v_prev_cumulative_deposits + NEW.deposit;

        -- Calculate value after previous flow using price at that time
        v_prev_value_after_flow := v_prev_cumulative_units * v_prev_price;

        -- Calculate current value before the cash flow using current price
        v_value_before_flow := v_prev_cumulative_units * v_current_price;

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
BEFORE INSERT ON user_cash_flow
FOR EACH ROW
EXECUTE FUNCTION calculate_incremental_twr();
