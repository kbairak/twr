import datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from performance.models import Cashflow


def test_derive_units_delta_from_execution_money_and_price() -> None:
    """units_delta should be derived from execution_money / execution_price"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        # units_delta is missing
        execution_price=Decimal("100.000000"),
        execution_money=Decimal("500.000000"),
        user_money=Decimal("505.000000"),
        fees=Decimal("5.000000"),
    )
    assert cf.units_delta == Decimal("5.000000")  # 500 / 100


def test_derive_execution_price_from_execution_money_and_units() -> None:
    """execution_price should be derived from execution_money / units_delta"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        units_delta=Decimal("5.000000"),
        # execution_price is missing
        execution_money=Decimal("500.000000"),
        user_money=Decimal("505.000000"),
        fees=Decimal("5.000000"),
    )
    assert cf.execution_price == Decimal("100.000000")  # 500 / 5


def test_derive_execution_money_from_units_and_price() -> None:
    """execution_money should be derived from units_delta * execution_price"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        units_delta=Decimal("5.000000"),
        execution_price=Decimal("100.000000"),
        # execution_money is missing
        user_money=Decimal("505.000000"),
        fees=Decimal("5.000000"),
    )
    assert cf.execution_money == Decimal("500.000000")  # 5 * 100


def test_derive_execution_money_from_user_money_and_fees() -> None:
    """execution_money should be derived from user_money - fees"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        units_delta=Decimal("5.000000"),
        execution_price=Decimal("100.000000"),
        # execution_money is missing
        user_money=Decimal("505.000000"),
        fees=Decimal("5.000000"),
    )
    assert cf.execution_money == Decimal("500.000000")  # 505 - 5


def test_derive_user_money_from_execution_money_and_fees() -> None:
    """user_money should be derived from execution_money + fees"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        units_delta=Decimal("5.000000"),
        execution_price=Decimal("100.000000"),
        execution_money=Decimal("500.000000"),
        # user_money is missing
        fees=Decimal("5.000000"),
    )
    assert cf.user_money == Decimal("505.000000")  # 500 + 5


def test_derive_fees_from_user_money_and_execution_money() -> None:
    """fees should be derived from user_money - execution_money"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        units_delta=Decimal("5.000000"),
        execution_price=Decimal("100.000000"),
        execution_money=Decimal("500.000000"),
        user_money=Decimal("505.000000"),
        # fees is missing
    )
    assert cf.fees == Decimal("5.000000")  # 505 - 500


def test_chained_derivation_all_from_three_values() -> None:
    """Should derive execution_money and user_money from units_delta, execution_price, and fees"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        units_delta=Decimal("5.000000"),
        execution_price=Decimal("100.000000"),
        # execution_money is missing - will be derived from units * price
        # user_money is missing - will be derived from execution_money + fees
        fees=Decimal("5.000000"),
    )
    assert cf.execution_money == Decimal("500.000000")  # 5 * 100
    assert cf.user_money == Decimal("505.000000")  # 500 + 5


def test_chained_derivation_from_user_money_and_fees() -> None:
    """Should derive execution_money, then units_delta from execution_money and price"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        # units_delta is missing
        execution_price=Decimal("100.000000"),
        # execution_money is missing - will be derived from user_money - fees
        user_money=Decimal("505.000000"),
        fees=Decimal("5.000000"),
    )
    assert cf.execution_money == Decimal("500.000000")  # 505 - 5
    assert cf.units_delta == Decimal("5.000000")  # 500 / 100


def test_chained_derivation_complex() -> None:
    """Should derive execution_price, then user_money from chained calculations"""
    cf = Cashflow(
        user_id=uuid4(),
        product_id=uuid4(),
        timestamp=datetime.datetime.now(),
        units_delta=Decimal("5.000000"),
        # execution_price is missing - will be derived from execution_money / units
        execution_money=Decimal("500.000000"),
        # user_money is missing - will be derived from execution_money + fees
        fees=Decimal("5.000000"),
    )
    assert cf.execution_price == Decimal("100.000000")  # 500 / 5
    assert cf.user_money == Decimal("505.000000")  # 500 + 5


def test_error_insufficient_data() -> None:
    """Should raise ValueError when not enough data to derive all values"""
    with pytest.raises(ValueError, match="Cannot derive mising values"):
        Cashflow(
            user_id=uuid4(),
            product_id=uuid4(),
            timestamp=datetime.datetime.now(),
            # Only providing units_delta - not enough to derive other values
            units_delta=Decimal("5.000000"),
        )


def test_error_insufficient_data_no_execution_money_path() -> None:
    """Should raise ValueError when execution_money cannot be derived"""
    with pytest.raises(ValueError, match="Cannot derive mising values"):
        Cashflow(
            user_id=uuid4(),
            product_id=uuid4(),
            timestamp=datetime.datetime.now(),
            # No way to derive execution_money from these alone
            units_delta=Decimal("5.000000"),
            fees=Decimal("5.000000"),
        )


def test_validation_error_units_price_mismatch() -> None:
    """Should raise ValueError when units_delta * execution_price != execution_money"""
    with pytest.raises(ValueError, match="Invalid cashflow.*units_delta.*!="):
        Cashflow(
            user_id=uuid4(),
            product_id=uuid4(),
            timestamp=datetime.datetime.now(),
            units_delta=Decimal("5.000000"),
            execution_price=Decimal("100.000000"),
            execution_money=Decimal("600.000000"),  # Should be 500, not 600
            user_money=Decimal("605.000000"),
            fees=Decimal("5.000000"),
        )


def test_validation_error_money_fees_mismatch() -> None:
    """Should raise ValueError when execution_money + fees != user_money"""
    with pytest.raises(ValueError, match="Invalid cashflow.*execution_money.*fees.*!="):
        Cashflow(
            user_id=uuid4(),
            product_id=uuid4(),
            timestamp=datetime.datetime.now(),
            units_delta=Decimal("5.000000"),
            execution_price=Decimal("100.000000"),
            execution_money=Decimal("500.000000"),
            user_money=Decimal("600.000000"),  # Should be 505, not 600
            fees=Decimal("5.000000"),
        )
