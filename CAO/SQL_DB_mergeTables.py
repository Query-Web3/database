#!/usr/bin/env python3
# SQL_DB_mergeTables.py
import mysql.connector
from mysql.connector import errorcode
from logging_config import logger
import pandas as pd
import numpy as np
import json
import datetime
import decimal
import math
import os
from dotenv import load_dotenv
from dotenv import load_dotenv
from utils import retry, DataValidator

class SQL_DB_MergeTables:
    """
    Usage:
        db = SQL_DB_MergeTables(userName="root", passWord="pwd", dataBase="mydb", initializeTable=True)
        db.run_merge()
    """

    def __init__(self, userName, passWord, host, dataBase, port, initializeTable=False):
        self.userName = userName
        self.passWord = passWord
        self.dataBase = dataBase
        self.port = port
        self.host = host
        if initializeTable:
            self.initialize_tables()

    # ---------- Schema (append-only) ----------
    def initialize_tables(self):
        """
        multipleFACT(
          id INT AUTO_INCREMENT PRIMARY KEY,
          payload JSON NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        create_sql = """
        CREATE TABLE IF NOT EXISTS multipleFACT (
            id INT AUTO_INCREMENT PRIMARY KEY,
            payload JSON NOT NULL,
            data_hash VARCHAR(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        self.executeSQL(create_sql)
        
        self.executeSQL(create_sql)
        
        # Ensure hash column exists (idempotent check)
        check_col_sql = """
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'multipleFACT' AND COLUMN_NAME = 'data_hash'
        """
        res = self.executeSQL(check_col_sql, (self.dataBase,))
        if res and res[0][0] == 0:
            self.executeSQL("ALTER TABLE multipleFACT ADD COLUMN data_hash VARCHAR(64);")
            logger.info("Added 'data_hash' column to multipleFACT")

        self._maybe_migrate_legacy_schema()

    def _maybe_migrate_legacy_schema(self):
        cols = self.executeSQL("""
            SELECT COLUMN_NAME, COLUMN_KEY, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'multipleFACT'
            ORDER BY ORDINAL_POSITION
        """, (self.dataBase,))
        colnames = {c[0]: {"key": c[1], "type": c[2]} for c in cols} if cols else {}

        pk_info = self.executeSQL("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'multipleFACT' AND CONSTRAINT_NAME = 'PRIMARY'
        """, (self.dataBase,))
        pk_cols = [r[0] for r in pk_info] if pk_info else []

        if pk_cols and not (len(pk_cols) == 1 and pk_cols[0] == 'id'):
            try:
                self.executeSQL("ALTER TABLE multipleFACT DROP PRIMARY KEY;")
            except Exception:
                pass

        if 'id' not in colnames:
            try:
                self.executeSQL("ALTER TABLE multipleFACT ADD COLUMN id INT FIRST;")
            except Exception:
                pass
        try:
            self.executeSQL("ALTER TABLE multipleFACT MODIFY COLUMN id INT NOT NULL AUTO_INCREMENT;")
        except Exception:
            pass
        try:
            pk_info2 = self.executeSQL("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'multipleFACT' AND CONSTRAINT_NAME = 'PRIMARY'
            """, (self.dataBase,))
            pk_cols2 = [r[0] for r in pk_info2] if pk_info2 else []
            if not pk_cols2:
                self.executeSQL("ALTER TABLE multipleFACT ADD PRIMARY KEY (id);")
        except Exception:
            pass

        cols_after = self.executeSQL("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'multipleFACT'
        """, (self.dataBase,))
        have_cols = {r[0] for r in cols_after} if cols_after else set()
        if 'created_at' not in have_cols:
            try:
                self.executeSQL("""
                    ALTER TABLE multipleFACT
                    ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP AFTER payload;
                """)
            except Exception:
                pass

    # ---------- DB helpers ----------
    def errorMessage(self, message):
        logger.error(f"SQL Error: {message}")

    @retry(max_retries=3, delay=2)
    def _connect(self):
        return mysql.connector.connect(
            user=self.userName,
            password=self.passWord,
            host=self.host,
            database=self.dataBase, 
            port=self.port
        )

    @retry(max_retries=3, delay=2)
    def executeSQL(self, query, params=None):
        try:
            cnx = self._connect()
            cursor = cnx.cursor()
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            try:
                values = cursor.fetchall()
            except Exception:
                values = None
            cnx.commit()
            cursor.close()
            cnx.close()
            return values
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.errorMessage("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.errorMessage("Database does not exist")
            else:
                self.errorMessage(str(err))
            raise
        except Exception as err:
            logger.exception(err)
            raise

    @retry(max_retries=3, delay=2)
    def fetch_df(self, query, params=None) -> pd.DataFrame:
        try:
            cnx = self._connect()
            cur = cnx.cursor()
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)
            rows = cur.fetchall()
            colnames = [d[0] for d in cur.description] if cur.description else []
            df = pd.DataFrame(rows, columns=colnames)
            cur.close()
            cnx.close()
            return df
        except Exception as err:
            logger.exception(err)
            raise

    @retry(max_retries=3, delay=2)
    def fetch_one(self, query, params=None):
        cnx = self._connect()
        cur = cnx.cursor()
        try:
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)
            return cur.fetchone()
        finally:
            cur.close()
            cnx.close()

    # ---------- JSON utils (strict sanitization) ----------
    @staticmethod
    def _json_default(o):
        if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
            return o.isoformat()
        if isinstance(o, decimal.Decimal):
            try:
                return float(o)
            except Exception:
                return str(o)
        return o

    def _sanitize_scalar(self, val):
        if val is None:
            return None
        if isinstance(val, (float, np.floating)):
            if not math.isfinite(val) or abs(val) > 1e308:
                return None
            return float(val)
        if isinstance(val, (int, np.integer)):
            return int(val)
        if isinstance(val, decimal.Decimal):
            try:
                f = float(val)
            except Exception:
                return None
            if not math.isfinite(f) or abs(f) > 1e308:
                return None
            return f
        if isinstance(val, (bool, np.bool_)):
            return bool(val)
        return val

    def _deep_clean(self, obj):
        if isinstance(obj, dict):
            return {k: self._deep_clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._deep_clean(v) for v in obj]
        return self._sanitize_scalar(obj)

    def _df_to_json_array(self, df: pd.DataFrame):
        if df is None or df.empty:
            return []
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.astype(object)
        df = df.where(pd.notna(df), None)
        df = df.applymap(self._sanitize_scalar)
        records = df.to_dict(orient='records')
        records = self._deep_clean(records)
        return records

    # ---------- Bifrost data (latest common batch_id + price via st.symbol) ----------
    Q_BIFROST_DATA = """
    SELECT
      s.Asset,
      s.tvl,
      s.apy,
      s.apyBase,
      s.apyReward,
      st.price
    FROM Bifrost_site_table AS s
    JOIN (
      SELECT s2.batch_id
      FROM Bifrost_site_table AS s2
      JOIN Bifrost_staking_table AS st2
        ON st2.batch_id = s2.batch_id
      ORDER BY s2.created_at DESC, s2.batch_id DESC
      LIMIT 1
    ) AS latest_common
      ON s.batch_id = latest_common.batch_id
    LEFT JOIN Bifrost_staking_table AS st
      ON st.batch_id = s.batch_id
     AND st.symbol   = s.Asset;
    """

    # ---------- Moonbeam/pool data ----------
    Q_POOLS_DATA = """
    SELECT token0_symbol, amount_token0, token1_symbol, amount_token1,
           volume_usd_current, volume_usd_24h, pools_apr, farming_apr, final_apr
    FROM pool_data
    WHERE batch_id = (
      SELECT batch_id
      FROM pool_data
      ORDER BY created_at DESC, batch_id DESC
      LIMIT 1
    );
    """

    # ---------- Hydration data ----------
    Q_HYDRATION_DATA = """
    SELECT asset_id, symbol, farm_apr, pool_apr, total_apr, tvl_usd, volume_usd
    FROM hydration_data
    WHERE batch_id = (
      SELECT batch_id
      FROM hydration_data
      ORDER BY created_at DESC, batch_id DESC
      LIMIT 1
    );
    """

    # ---------- Hydration_price (latest batch) ----------
    Q_HYDRATION_PRICE_DATA = """
    SELECT asset_id, symbol, price_usdt
    FROM Hydration_price
    WHERE batch_id = (
      SELECT batch_id
      FROM Hydration_price
      ORDER BY created_at DESC, batch_id DESC
      LIMIT 1
    );
    """

    # ---------- Bifrost × Hydration (latest of each, inner join on Asset=symbol with clear aliases) ----------
    Q_BIFROST_HYDRATION_COMBINED = """
    SELECT
      s.Asset                                  AS Asset,
      s.tvl                                    AS Bifrost_tvl,
      s.apy                                    AS Bifrost_apy,
      s.apyBase                                AS Bifrost_apyBase,
      s.apyReward                              AS Bifrost_apyReward,
      h.asset_id                               AS Hydration_asset_id,
      h.symbol                                 AS Hydration_symbol,
      h.farm_apr                               AS Hydration_farm_apr,
      h.pool_apr                               AS Hydration_pool_apr,
      h.total_apr                              AS Hydration_total_apr,
      h.tvl_usd                                AS Hydration_tvl_usd,
      h.volume_usd                             AS Hydration_volume_usd
    FROM (
      SELECT Asset, tvl, apy, apyBase, apyReward
      FROM Bifrost_site_table
      WHERE batch_id = (
        SELECT batch_id
        FROM Bifrost_site_table
        ORDER BY created_at DESC, batch_id DESC
        LIMIT 1
      )
    ) AS s
    JOIN (
      SELECT asset_id, symbol, farm_apr, pool_apr, total_apr, tvl_usd, volume_usd
      FROM hydration_data
      WHERE batch_id = (
        SELECT batch_id
        FROM hydration_data
        ORDER BY created_at DESC, batch_id DESC
        LIMIT 1
      )
    ) AS h
      ON h.symbol = s.Asset;
    """

    # ---------- Metadata ----------
    Q_BIFROST_META = """
    SELECT batch_id, created_at
    FROM Bifrost_site_table
    ORDER BY created_at DESC, batch_id DESC
    LIMIT 1;
    """
    Q_POOLS_META = """
    SELECT batch_id, created_at
    FROM pool_data
    ORDER BY created_at DESC, batch_id DESC
    LIMIT 1;
    """
    Q_HYDRATION_META = """
    SELECT batch_id, created_at
    FROM hydration_data
    ORDER BY created_at DESC, batch_id DESC
    LIMIT 1;
    """
    Q_HYDRATION_PRICE_META = """
    SELECT batch_id, created_at
    FROM Hydration_price
    ORDER BY created_at DESC, batch_id DESC
    LIMIT 1;
    """

    # ---------- Insert (append-only) ----------
    def insert_combined_payload(self, payload_obj: dict, data_hash: str):
        payload_str = json.dumps(payload_obj, default=self._json_default, ensure_ascii=False, allow_nan=False)
        self.executeSQL(
            "INSERT INTO multipleFACT (payload, data_hash) VALUES (%s, %s);",
            (payload_str, data_hash)
        )

    def get_last_merge_hash(self):
        query = "SELECT data_hash FROM multipleFACT ORDER BY id DESC LIMIT 1"
        res = self.executeSQL(query)
        if res and res[0][0]:
            return res[0][0]
        return None

    # ---------- High-level API ----------
    def run_merge(self):
        # Ensure table exists / migrate if legacy
        self.initialize_tables()

        # Fetch dataframes
        df_bifrost    = self.fetch_df(self.Q_BIFROST_DATA)
        df_pools      = self.fetch_df(self.Q_POOLS_DATA)
        df_hydration  = self.fetch_df(self.Q_HYDRATION_DATA)
        df_h_price    = self.fetch_df(self.Q_HYDRATION_PRICE_DATA)
        df_bxhy       = self.fetch_df(self.Q_BIFROST_HYDRATION_COMBINED)

        # Sanitize → lists of dicts
        bifrost_records    = self._df_to_json_array(df_bifrost)
        moonbeam_records   = self._df_to_json_array(df_pools)
        hydration_records  = self._df_to_json_array(df_hydration)
        hydration_price_records = self._df_to_json_array(df_h_price)
        bxhy_records       = self._df_to_json_array(df_bxhy)

        # Metadata
        bifrost_meta    = self.fetch_one(self.Q_BIFROST_META)
        pools_meta      = self.fetch_one(self.Q_POOLS_META)
        hydration_meta  = self.fetch_one(self.Q_HYDRATION_META)
        hydration_price_meta = self.fetch_one(self.Q_HYDRATION_PRICE_META)

        batch_id_bifrost, created_at_bifrost = (None, None)
        batch_id_moonbeam, created_at_moonbeam = (None, None)
        batch_id_hydration, created_at_hydration = (None, None)
        batch_id_hydration_price, created_at_hydration_price = (None, None)

        if bifrost_meta:
            batch_id_bifrost, created_at_bifrost = bifrost_meta[0], bifrost_meta[1]
        if pools_meta:
            batch_id_moonbeam, created_at_moonbeam = pools_meta[0], pools_meta[1]
        if hydration_meta:
            batch_id_hydration, created_at_hydration = hydration_meta[0], hydration_meta[1]
        if hydration_price_meta:
            batch_id_hydration_price, created_at_hydration_price = hydration_price_meta[0], hydration_price_meta[1]

        def _dt(x):
            return x if isinstance(x, datetime.datetime) else None

        dt_b = _dt(created_at_bifrost)
        dt_m = _dt(created_at_moonbeam)
        dt_h = _dt(created_at_hydration)
        dt_hp = _dt(created_at_hydration_price)
        combined_dt = max([d for d in [dt_b, dt_m, dt_h, dt_hp] if d is not None], default=None)

        payload_obj = {
            # Bifrost
            "batch_id_bifrost": batch_id_bifrost,
            "created_at_bifrost": created_at_bifrost.isoformat() if isinstance(created_at_bifrost, datetime.datetime) else created_at_bifrost,
            "bifrost_data": bifrost_records,

            # Moonbeam / pools
            "batch_id_moonbeam": batch_id_moonbeam,
            "created_at_moonbeam": created_at_moonbeam.isoformat() if isinstance(created_at_moonbeam, datetime.datetime) else created_at_moonbeam,
            "moonbeam_data": moonbeam_records,

            # Hydration data
            "batch_id_hydration": batch_id_hydration,
            "created_at_hydration": created_at_hydration.isoformat() if isinstance(created_at_hydration, datetime.datetime) else created_at_hydration,
            "hydration_data": hydration_records,

            # Hydration price
            "batch_id_hydration_price": batch_id_hydration_price,
            "created_at_hydration_price": created_at_hydration_price.isoformat() if isinstance(created_at_hydration_price, datetime.datetime) else created_at_hydration_price,
            "hydration_price_data": hydration_price_records,

            # Combined Bifrost × Hydration
            "bifrost_hydration_data": bxhy_records,

            # Combined timestamp helper
            "combined_created_at": combined_dt.isoformat() if combined_dt else None
        }

        payload_obj = self._deep_clean(payload_obj)
        
        # Deduplication
        current_hash = DataValidator.compute_hash(payload_obj)
        last_hash = self.get_last_merge_hash()
        
        if current_hash and current_hash == last_hash:
            logger.info("Duplicate merged data detected. Skipping insertion.")
            return

        self.insert_combined_payload(payload_obj, current_hash)
        logger.info("Inserted new combined snapshot into multipleFACT (append-only).")


if __name__ == "__main__":
    load_dotenv()
    user = os.getenv("DB_USERNAME", "root")
    pwd = os.getenv("DB_PASSWORD", "")
    dbn = os.getenv("DB_NAME", "test")
    init = os.getenv("INIT_TABLES", "1") == "1"

    db = SQL_DB_MergeTables(userName=user, passWord=pwd, dataBase=dbn, initializeTable=init)
    db.run_merge()
