import argparse
import asyncio
import aiohttp
import hashlib
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Constants for API interaction and data storage
BASE_URL = "https://openexchangerates.org/api/"
API_KEY = {YOUR_API_KEY}  # Replace with your API key
API_VERSION = "v1"
DEFAULT_SAVE_PATH = "output"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Function to apply exponential backoff for retries
def exponential_backoff(retry_count):
    """
    Apply an exponential backoff strategy for retrying API requests.

    Args:
        retry_count (int): The number of retry attempts made.

    Returns:
        int: The delay time in seconds before the next retry.
    """
    return min(2**retry_count, 60)


async def fetch_exchange_rates(session, date, retries=5):
    """
    Fetch historical exchange rates for a specific date with retry handling.

    Args:
        session (aiohttp.ClientSession): The aiohttp session for making requests.
        date (str): The date for which exchange rates are needed in YYYY-MM-DD format.
        retries (int): Maximum number of retry attempts in case of failures.

    Returns:
        dict or None: The JSON response containing exchange rates, or None if unsuccessful.
    """
    url = f"{BASE_URL}historical/{date}.json?app_id={API_KEY}"

    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()  # Successfully retrieved data
                elif response.status == 401:
                    logging.error(
                        f"Unauthorized request (401) for {date}. Check API key."
                    )
                    return None
                elif response.status == 429:  # Rate limit exceeded
                    wait_time = exponential_backoff(attempt)
                    logging.warning(
                        f"Rate limit hit. Retrying in {wait_time} seconds..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(f"Error fetching data for {date}: {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Exception fetching data for {date}: {e}")
            await asyncio.sleep(exponential_backoff(attempt))

    return None


async def fetch_latest_exchange_rates(session):
    """
    Fetch the most recent exchange rates.

    Args:
        session (aiohttp.ClientSession): The aiohttp session for making requests.

    Returns:
        dict or None: The JSON response containing exchange rates, or None if unsuccessful.
    """
    url = f"{BASE_URL}latest.json?app_id={API_KEY}"
    async with session.get(url) as response:
        if response.status == 200:
            return await response.json()  # Successfully retrieved latest exchange rates
        else:
            logging.error(f"Error fetching latest exchange rates: {response.status}")
            return None


def generate_id(date, currency, rate):
    """
    Generate a unique hash-based identifier for each exchange rate entry.

    Args:
        date (str): The date of the exchange rate.
        currency (str): The target currency.
        rate (float): The exchange rate value.

    Returns:
        str: A SHA-256 hash representing the unique entry.
    """
    unique_str = f"{date}_{currency}_{rate}"
    return hashlib.sha256(unique_str.encode()).hexdigest()


def save_to_parquet(df, save_path):
    """
    Save exchange rate data to a Parquet file with partitioning by year and month.

    Args:
        df (pandas.DataFrame): The DataFrame containing exchange rate data.
        save_path (str): The directory path to save the Parquet files.
    """
    Path(save_path).mkdir(parents=True, exist_ok=True)  # Ensure the save path exists

    for (year, month), month_df in df.groupby(["year", "month"]):
        partitioned_path = Path(save_path) / f"year={year}" / f"month={month}"
        partitioned_path.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(month_df), partitioned_path / "exchange_rates.parquet"
        )
        logging.info(f"Data saved to {partitioned_path}/exchange_rates.parquet")


async def main(start_date, end_date, save_path):
    """
    Main function to fetch, process, and save exchange rate data.

    Args:
        start_date (str or None): Start date for fetching historical rates (YYYY-MM-DD format).
        end_date (str or None): End date for fetching historical rates (YYYY-MM-DD format).
        save_path (str): Directory path to store the processed data.
    """
    save_path = save_path or DEFAULT_SAVE_PATH  # Use default path if not specified
    logging.info(f"Saving data to: {save_path}")

    all_data = []
    async with aiohttp.ClientSession() as session:
        if start_date and end_date:
            # Convert string dates to datetime objects
            current_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            # Create tasks for fetching historical exchange rates
            tasks = [
                fetch_exchange_rates(
                    session, (current_date + timedelta(days=i)).strftime("%Y-%m-%d")
                )
                for i in range((end_date - current_date).days + 1)
            ]
            responses = await asyncio.gather(*tasks)
        else:
            responses = [
                await fetch_latest_exchange_rates(session)
            ]  # Fetch latest rates

        # Process fetched data
        for response in responses:
            if response:
                base_currency = response.get("base", "USD")
                retrieval_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                timestamp = response.get("timestamp")
                date_str = (
                    datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d")
                    if timestamp
                    else datetime.utcnow().strftime("%Y-%m-%d")
                )
                day_of_week = datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")
                is_weekend = day_of_week in ["Saturday", "Sunday"]

                for currency, rate in response.get("rates", {}).items():
                    row_id = generate_id(date_str, currency, rate)
                    all_data.append(
                        [
                            row_id,
                            date_str,
                            base_currency,
                            currency,
                            rate,
                            retrieval_time,
                            API_VERSION,
                            day_of_week,
                            is_weekend,
                            int(date_str[:4]),
                            int(date_str[5:7]),
                        ]
                    )

    if all_data:
        df = pd.DataFrame(
            all_data,
            columns=[
                "id",
                "date",
                "base_currency",
                "currency",
                "exchange_rate",
                "retrieval_time",
                "source_api_version",
                "day_of_week",
                "is_weekend",
                "year",
                "month",
            ],
        )
        save_to_parquet(df, save_path)
        print(df.to_string(index=False))
    else:
        logging.warning("No data retrieved.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch historical or latest exchange rates and save as Parquet with metadata."
    )
    parser.add_argument(
        "--start-date", help="Start date in YYYY-MM-DD format (optional)"
    )
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD format (optional)")
    parser.add_argument(
        "--save-path",
        help="Path to save the Parquet dataset (optional, defaults to /output)",
    )

    args = parser.parse_args()

    if not args.start_date or not args.end_date:
        logging.info("No date range provided, fetching latest exchange rates.")

    asyncio.run(main(args.start_date, args.end_date, args.save_path))
