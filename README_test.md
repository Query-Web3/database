# QueryWeb3 Database Project - Test Suite

This directory contains a comprehensive functional test suite for the Python scripts in the `CAO` folder.

## Prerequisites

- Python 3.x
- Dependencies listed in the project (pandas, numpy, requests, mysql-connector-python, python-dotenv)

## Running the Tests

To run the full test suite, execute the following command from the **root directory** of the project:

```bash
python3 -m unittest test_all_functions.py
```

## What is Tested?

The `test_all_functions.py` script covers:

1.  **Bifrost Data Fetching**:
    - API response parsing (`fetch_data`, `fetch_data2`)
    - DataFrame sanitization (`sanitize_df`)
2.  **Hydration Data Fetching**:
    - APR calculations (Pool APR, Total APR)
    - Data processing logic
3.  **SQL Database Classes**:
    - Connection handling (mocked)
    - Query execution structure
4.  **Asset Prices**:
    - Price matching logic
5.  **StellaSwap**:
    - Token amount calculations
    - APR fetching logic
6.  **Combined Tables**:
    - Helper functions like `_to_decimal`

## Notes

- **Mocking**: All external network requests (API calls) and database connections are **mocked**. This means the tests do **not** require a running MySQL database or internet access to pass. They test the *logic* of the code, not the external systems.
- **Environment Variables**: The test suite automatically sets dummy environment variables (`DB_USERNAME`, etc.) so that the scripts can be imported without errors, even if you don't have a `.env` file set up locally.
