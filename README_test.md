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

The tests are located in the `tests/` directory and are divided into unit and integration/E2E tests:

### Unit Tests (Fast, Mocked)
- `test_sql_db.py`: Core database connection and base execution logic.
- `test_data_quality.py`: Unit tests for data validation, hashing utilities, and batch ID generation.
- `test_health_checks.py`: Verification of system health monitoring utilities.
- `test_error_handling.py`: Tests for the retry decorator and common error handling logic.
- `test_bifrost_fetching.py`: Bifrost API response parsing and sanitization.
- `test_hydration_fetching.py`: Hydration pool TVL and volume processing logic.
- `test_stellaswap.py`: Stellaswap graph data and farming APR logic.
- `test_combine_tables.py`: Integration logic for merging multiple data sources.
- `test_all_data_jobs.py`: Orchestration logic for the `JobOrchestrator` class.
- `test_fetch_asset_prices.py`: Logic for price fetching and normalization.

### Integration & E2E Tests (Require Live DB)
- `test_integration_live.py`: Runs real data pipelines against a test database (`QUERYWEB3`) to verify the full flow from API to Table.
- `test_data_integrity_live.py`: Verifies MySQL persistence, hash-based **deduplication**, and correct handling of `NULL` values.
- `test_all_data_jobs_e2e.py`: Validates orchestrator signal handling (SIGINT/SIGTERM), child process management, and graceful cleanup.
- `test_migration_logic.py`: Verifies that the database schema migration system tracks versions correctly and is idempotent.

## Key Features

- **Data Integrity**: Unlike simple unit tests, the integration suite queries a live database to ensure that data is actually stored correctly.
- **Deduplication**: Tests verify that identical data is detected via SHA256 hashes and not re-inserted, saving database space.
- **Graceful Shutdown**: E2E tests verify that stopping the orchestrator (Ctrl+C) reaps all child processes, preventing zombie PIDs.
- **Mock Runners**: Uses `mock_long_runner.py` to simulate long-running jobs in a controlled environment.
- **Environment Safety**: Integration tests use a `TEST_` prefix for tables or a dedicated test database to avoid polluting production data.

## Continuous Integration

Before submitting changes, ensure all tests pass:
```bash
python -m pytest tests/
```
