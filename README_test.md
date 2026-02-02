# QueryWeb3 Database Project - Test Suite

This project uses a comprehensive test suite built with `pytest` to ensure the reliability of the blockchain data pipelines.

## Prerequisites

- Python 3.x
- `pytest` and `pytest-cov` installed:
  ```bash
  pip install pytest pytest-cov
  ```
- Other project dependencies (pandas, numpy, requests, mysql-connector-python, python-dotenv)

## Running the Tests

To run the full test suite from the **root directory**:

```bash
python -m pytest tests/
```

### Coverage Report

To run tests and see a detailed coverage report for the `CAO` folder:

```bash
python -m pytest tests/ --cov=CAO --cov-report=term-missing
```

The current project documentation and logic achievement is **81% total coverage**.

## Test Structure

The tests are located in the `tests/` directory and follow a modular structure:

- `test_sql_db.py`: Core database connection and base execution logic.
- `test_data_quality.py`: Unit tests for data validation, hashing utilities, and batch ID generation.
- `test_health_checks.py`: Verification of system health monitoring utilities (DB connection, process status, disk space).
- `test_error_handling.py`: Tests for the retry decorator and common error handling logic.
- `test_bifrost_fetching.py`: Bifrost API response parsing and sanitization.
- `test_hydration_fetching.py`: Hydration pool TVL and volume processing.
- `test_stellaswap.py`: Stellaswap graph data and farming APR logic.
- `test_combined_tables.py`: Integration logic for merging multiple data sources.
- `test_all_data_jobs.py`: Orchestration and long-running job management.
- `test_certified_80.py`: Comprehensive coverage booster targeting complex logic branches.

## Key Features

- **Mocking**: All external network requests (API calls) and database connections are **mocked**. No live database or internet access is required.
- **Robustness**: The suite tests error handling, signal termination, and data edge cases (NaNs, empty responses).
- **Environment Isolation**: The tests automatically handle mock configurations, so your local `.env` file is not affected.

## Continuous Integration

Before submitting changes, ensure all tests pass:
```bash
python -m pytest tests/
```
