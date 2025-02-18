import pytest
import asyncio
import aiohttp
import pandas as pd
import hashlib
import shutil
from pathlib import Path
from exchange_rate_pipeline import (
    generate_id,
    save_to_parquet,
    fetch_exchange_rates,
    fetch_latest_exchange_rates,
)


@pytest.mark.parametrize(
    "date, currency, rate", [("2024-02-18", "EUR", 1.12), ("2023-12-25", "GBP", 0.85)]
)
def test_generate_id(date, currency, rate):
    expected_id = hashlib.sha256(f"{date}_{currency}_{rate}".encode()).hexdigest()
    assert generate_id(date, currency, rate) == expected_id


def test_save_to_parquet():
    data = {
        "id": ["123"],
        "date": ["2024-02-18"],
        "base_currency": ["USD"],
        "currency": ["EUR"],
        "exchange_rate": [1.12],
        "retrieval_time": ["2024-02-18 12:00:00"],
        "source_api_version": ["v1"],
        "day_of_week": ["Sunday"],
        "is_weekend": [True],
        "year": [2024],
        "month": [2],
    }
    df = pd.DataFrame(data)
    save_path = "test_output"
    save_to_parquet(df, save_path)

    # Check if file exists
    partitioned_path = (
        Path(save_path) / "year=2024" / "month=2" / "exchange_rates.parquet"
    )
    assert partitioned_path.exists()

    # Clean up safely
    shutil.rmtree(save_path, ignore_errors=True)


@pytest.mark.asyncio
async def test_fetch_exchange_rates():
    async with aiohttp.ClientSession() as session:
        response = await fetch_exchange_rates(session, "2024-02-18")
        assert isinstance(response, dict)
        assert "rates" in response


@pytest.mark.asyncio
async def test_fetch_latest_exchange_rates():
    async with aiohttp.ClientSession() as session:
        response = await fetch_latest_exchange_rates(session)
        assert isinstance(response, dict)
        assert "rates" in response
