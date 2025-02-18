# Exchange Rate Pipeline

## Overview
This project fetches historical or latest exchange rate data from the Open Exchange Rates API and stores it in Parquet format, partitioned by year and month. The project includes:
- `exchange_rate_pipeline.py`: Fetches exchange rates, processes them, and saves them in partitioned Parquet files.
- `test_exchange_rate_pipeline.py`: Unit tests for the pipeline using `pytest` and `pytest-asyncio`.

## Installation & Setup

### 1️. Create a Virtual Environment
```sh
python -m venv venv
source venv/bin/activate  # On macOS/Linux
venv\Scripts\activate     # On Windows
```

### 2️. Install Dependencies
Ensure you have all required dependencies by running:
```sh
pip install -r requirements.txt
```

### 3️. API Key Configuration
You **must** provide your own Open Exchange Rates API key. Update the `API_KEY` variable in `exchange_rate_pipeline.py` at **line 14**:
```python
API_KEY = {YOUR_API_KEY}
```

## Running the Exchange Rate Pipeline

### Command Line Arguments
The script supports the following command line arguments:

- `--start-date`: (Optional) Start date in `YYYY-MM-DD` format. Required for fetching historical data.
- `--end-date`: (Optional) End date in `YYYY-MM-DD` format. Required for fetching historical data.
- `--save-path`: (Optional) Path to save the Parquet dataset. Defaults to `output/` if not provided.

**NOTE:** `start-date` **AND** `end-date` are needed to fetch historical data. If one or neither is provided, only the last **30 days** of data will be made available.

### 1️. Fetch Historical Exchange Rates
```sh
python exchange_rate_pipeline.py --start-date=2024-01-01 --end-date=2024-01-31 --save-path=output/
```

### 2️. Fetch Latest Exchange Rates (No Date Range Needed)
```sh
python exchange_rate_pipeline.py --save-path=output/
```

## Example Output
When running the pipeline, you will see an output similar to this:

```
                id        date base_currency currency  exchange_rate     retrieval_time source_api_version day_of_week  is_weekend  year  month
5e2b2...  2024-02-18           USD      EUR          1.12  2024-02-18 12:00:00                v1    Sunday       True  2024      2
3a4b7...  2024-02-18           USD      GBP          0.85  2024-02-18 12:00:00                v1    Sunday       True  2024      2
```

## Running the Tests

### 1️. Run All Tests
```sh
pytest --asyncio-mode=auto -v
```

### 2️. Run a Specific Test
For example, to run only `test_generate_id`:
```sh
pytest -k test_generate_id -v
```

## Project Structure
```
.
├── exchange_rate_pipeline.py       # Main script for fetching exchange rates
├── test_exchange_rate_pipeline.py  # Pytest unit tests
├── requirements.txt                # Dependencies
├── README.md                       # Documentation
└── output/                         # Folder where Parquet files are saved
```

## Notes
- Ensure you have a valid Open Exchange Rates API key before running the script.
- The script automatically saves data in `output/` if no `--save-path` is provided.
- The test suite includes asynchronous tests and requires `pytest-asyncio`.
