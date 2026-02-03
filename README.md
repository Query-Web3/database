# Project Setup Guide

This repository contains the data ingestion pipeline and supporting scripts used to fetch, process, and store Web3 and Hydration ecosystem data.  
Follow the instructions below to install dependencies, configure your environment, and run the system.

---

## 1. Prerequisites

### Python
Python **3.5+** or later is required.

Install required Python libraries:

```
pip install pandas mysql-connector-python requests
```

---

## 2. MySQL Installation & Setup

### Install MySQL on macOS
```
brew install mysql
brew services start mysql
```

### Create MySQL User and Database

Log in to MySQL:

```
mysql -u root -p
```

Run the following SQL commands:

```
CREATE USER 'queryweb3'@'localhost' IDENTIFIED BY '!SDCsdin2df2@';

CREATE DATABASE QUERYWEB3;

GRANT ALL PRIVILEGES ON QUERYWEB3.* TO 'queryweb3'@'localhost';

FLUSH PRIVILEGES;
```

---

## 3. Environment Configuration

Create a `.env` file in the **CAO** folder with the following default values:

```
DB_USERNAME="queryweb3"
DB_PASSWORD="!SDCsdin2df2@"
DB_NAME="QUERYWEB3"
API_KEY="8d6080e3d9c214680a8543a1a29758c9"
```

You may modify these values if needed.

---

## 4. Hydration SDK Installation

This project uses the Hydration SDK from Galactic Council:  
https://github.com/galacticcouncil/sdk/tree/master/packages/sdk

Install and build the SDK:

```
cd CAO/hy 
npm install
sudo npm install -g tsx
```

---


## 5. Running the Data Pipeline

Once dependencies and environment variables are configured, run:

```
python all_data_jobs.py
```

This script fetches data from Web3 APIs, processes it, and stores it in MySQL.

---

## Notes

- Ensure MySQL is running before starting scripts.
- Rebuild the SDK after pulling updates (`npm run build`).
- If Hydration RPC is unavailable, switch to a different RPC endpoint.

---

