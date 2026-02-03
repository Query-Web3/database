#!/usr/bin/env python3
# SQL_DB_combinedTables.py
"""
Combine latest batches from hydration_data, pool_data, and Bifrost tables
into a unified append-only table: full_table.

Adds columns:
  - `chain`  : 'hydration' | 'moonbeam' | 'bifrost'
  - `price`  : matched from latest Hydration_price (by symbol), else from latest per-symbol Bifrost_staking_table, else NULL

Environment (.env) variables:
  DB_USERNAME, DB_PASSWORD, DB_HOST (default 127.0.0.1), DB_NAME

Usage:
  python SQL_DB_combinedTables.py            # run once
"""

import os
import json
import decimal
from typing import Any, Dict, List, Optional, Sequence

import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv
from logging_config import logger

Decimal = decimal.Decimal


def _to_decimal(v: Any) -> Optional[Decimal]:
    """Safe conversion to Decimal; returns None for '', 'nan', etc."""
    if v is None:
        return None
    try:
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        if isinstance(v, str):
            s = v.strip()
            if s == "" or s.lower() == "nan":
                return None
            # be tolerant to commas / percent
            s = s.replace(",", "").replace("%", "")
            return Decimal(s)
    except Exception:
        return None
    return None


class SQL_DB_CombinedTables:
    def __init__(self, user: str, password: str, db: str, db_port: int, host: str) -> None:
        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.port = db_port
        self.conn = None  # type: ignore

    # ---------- DB helpers ----------
    def connect(self) -> None:
        if self.conn:
            return
        
        self.conn = mysql.connector.connect(
            user=self.user, password=self.password, host=self.host, database=self.db, port=self.port
        )

    def cursor(self):
        self.connect()
        return self.conn.cursor(dictionary=True)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        cur = self.cursor()
        cur.execute(sql, params or ())
        if cur.with_rows:
            rows = cur.fetchall()
            cur.close()
            return rows
        self.conn.commit()
        cur.close()
        return []

    # ---------- Setup ----------
    def ensure_full_table(self) -> None:
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS full_table (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                source VARCHAR(64) NOT NULL,
                chain  VARCHAR(32) NOT NULL,
                batch_id BIGINT,
                symbol JSON NULL,
                farm_apy DECIMAL(40,18) NULL,
                pool_apy DECIMAL(40,18) NULL,
                apy DECIMAL(40,18) NULL,
                tvl DECIMAL(40,18) NULL,
                volume DECIMAL(40,18) NULL,
                tx BIGINT NULL,
                price DECIMAL(40,18) NULL,
                created_at DATETIME NULL,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        try:
            self.execute("ALTER TABLE full_table ADD COLUMN chain VARCHAR(32) NOT NULL DEFAULT 'hydration'")
        except MySQLError:
            pass
        try:
            self.execute("ALTER TABLE full_table ADD COLUMN price DECIMAL(40,18) NULL")
        except MySQLError:
            pass

    # ---------- Utility ----------
    def table_columns_lower(self, table: str) -> List[str]:
        rows = self.execute(
            """
            SELECT LOWER(column_name) AS cname
            FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = %s
            """,
            (table,),
        )
        return [r["cname"] for r in rows]

    def latest_batch_id(self, table: str) -> Optional[int]:
        rows = self.execute(
            f"""
            SELECT batch_id
            FROM `{table}`
            ORDER BY created_at DESC, batch_id DESC
            LIMIT 1
            """
        )
        return int(rows[0]["batch_id"]) if rows else None

    # ---------- Latest price map (Hydration primary, Bifrost staking fallback per symbol) ----------
    def latest_price_map(self) -> Dict[str, Decimal]:
        mp: Dict[str, Decimal] = {}

        # 1) Primary: Hydration_price (latest batch)
        try:
            hydr_batch = self.latest_batch_id("Hydration_price")
        except Exception as e:
            logger.warning(f"Hydration_price batch lookup failed: {e}")
            hydr_batch = None

        if hydr_batch is not None:
            try:
                rows = self.execute(
                    "SELECT symbol, price_usdt FROM `Hydration_price` WHERE batch_id = %s",
                    (hydr_batch,),
                )
                for r in rows:
                    sym = r.get("symbol")
                    price = _to_decimal(r.get("price_usdt"))
                    if sym and price is not None:
                        mp[str(sym).lower()] = price
                self._latest_price_batch = hydr_batch  # type: ignore[attr-defined]
            except MySQLError as e:
                logger.warning(f"Hydration_price read failed for batch {hydr_batch}: {e}")

        # 2) Fallback: latest-per-symbol from Bifrost_staking_table
        # (self-join version works on MySQL 5.7+; adds batch_id tie-breaker)
        try:
            rows = self.execute(
                """
                SELECT s.symbol, s.price
                FROM Bifrost_staking_table s
                JOIN (
                    SELECT symbol, MAX(created_at) AS max_created
                    FROM Bifrost_staking_table
                    WHERE symbol IS NOT NULL AND price IS NOT NULL
                    GROUP BY symbol
                ) m
                  ON m.symbol = s.symbol AND s.created_at = m.max_created
                JOIN (
                    SELECT s2.symbol, s2.created_at, MAX(s2.batch_id) AS max_batch
                    FROM Bifrost_staking_table s2
                    WHERE s2.symbol IS NOT NULL AND s2.price IS NOT NULL
                    GROUP BY s2.symbol, s2.created_at
                ) b
                  ON b.symbol = s.symbol AND b.created_at = s.created_at AND b.max_batch = s.batch_id
                """
            )
            for r in rows:
                sym = r.get("symbol")
                price = _to_decimal(r.get("price"))
                if sym and price is not None:
                    k = str(sym).lower()
                    if k not in mp:
                        mp[k] = price
        except MySQLError as e:
            logger.warning(f"Bifrost_staking_table latest-per-symbol read failed: {e}")

        return mp

    # ---------- Extractors ----------
    def rows_from_hydration(self, batch_id: int, price_map: Dict[str, Decimal]) -> List[Dict[str, Any]]:
        rows = self.execute(
            """
            SELECT batch_id, symbol, farm_apr, pool_apr, total_apr, tvl_usd, volume_usd, created_at
            FROM `hydration_data`
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            sym = r.get("symbol")
            price = price_map.get(str(sym).lower()) if sym is not None else None
            rec = dict(
                source="hydration_data",
                chain="hydration",
                batch_id=r.get("batch_id"),
                symbol=json.dumps({"symbol": sym}) if sym is not None else None,
                farm_apy=_to_decimal(r.get("farm_apr")),
                pool_apy=_to_decimal(r.get("pool_apr")),
                apy=_to_decimal(r.get("total_apr")),
                tvl=_to_decimal(r.get("tvl_usd")),
                volume=_to_decimal(r.get("volume_usd")),
                tx=None,
                price=price,
                created_at=r.get("created_at"),
            )
            out.append(rec)
        return out

    def rows_from_pool(self, batch_id: int, price_map: Dict[str, Decimal]) -> List[Dict[str, Any]]:
        rows = self.execute(
            """
            SELECT batch_id, token0_symbol, token1_symbol, farming_apr, pools_apr, final_apr,
                   volume_usd_24h, tx_count, created_at
            FROM `pool_data`
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            sym = {
                "token0_symbol": r.get("token0_symbol"),
                "token1_symbol": r.get("token1_symbol"),
            }
            rec = dict(
                source="pool_data",
                chain="moonbeam",
                batch_id=r.get("batch_id"),
                symbol=json.dumps(sym),
                farm_apy=_to_decimal(r.get("farming_apr")),
                pool_apy=_to_decimal(r.get("pools_apr")),
                apy=_to_decimal(r.get("final_apr")),
                tvl=None,
                volume=_to_decimal(r.get("volume_usd_24h")),
                tx=(int(r["tx_count"]) if r.get("tx_count") is not None else None),
                price=None,  # ambiguous for pools
                created_at=r.get("created_at"),
            )
            out.append(rec)
        return out

    # --- New: use Bifrost_site_table latest non-NULL per Asset (APY comes from site table) ---
    def rows_from_bifrost_site_latest(self, price_map: Dict[str, Decimal]) -> List[Dict[str, Any]]:
        """
        Pull the latest non-NULL APY row per Asset from Bifrost_site_table
        (independent of batch_id), and match price via price_map.
        """
        try:
            rows = self.execute(
                """
                SELECT t.Asset AS sym,
                       t.apyReward AS farming_apy,
                       t.apyBase   AS base_apy,
                       COALESCE(t.apy, t.apyBase + t.apyReward) AS total_apy,
                       t.tvl       AS tvl_val,
                       t.batch_id,
                       t.created_at
                FROM Bifrost_site_table t
                JOIN (
                    SELECT Asset, MAX(created_at) AS max_created
                    FROM Bifrost_site_table
                    WHERE Asset IS NOT NULL
                      AND LOWER(Asset) NOT IN ('tvl','addresses','revenue','bncprice')
                      AND (apy IS NOT NULL OR apyBase IS NOT NULL OR apyReward IS NOT NULL)
                    GROUP BY Asset
                ) m
                  ON m.Asset = t.Asset AND t.created_at = m.max_created
                JOIN (
                    SELECT Asset, created_at, MAX(batch_id) AS max_batch
                    FROM Bifrost_site_table
                    WHERE Asset IS NOT NULL
                      AND LOWER(Asset) NOT IN ('tvl','addresses','revenue','bncprice')
                      AND (apy IS NOT NULL OR apyBase IS NOT NULL OR apyReward IS NOT NULL)
                    GROUP BY Asset, created_at
                ) b
                  ON b.Asset = t.Asset AND b.created_at = t.created_at AND b.max_batch = t.batch_id
                """
            )
        except MySQLError as e:
            logger.warning(f"Bifrost_site_table latest-per-asset read failed: {e}")
            return []

        out: List[Dict[str, Any]] = []
        for r in rows:
            sym = r.get("sym")
            price = price_map.get(str(sym).lower()) if sym is not None else None
            rec = dict(
                source="Bifrost_site_table",
                chain="bifrost",
                batch_id=r.get("batch_id"),
                symbol=json.dumps({"symbol": sym}) if sym else None,
                farm_apy=_to_decimal(r.get("farming_apy")),
                pool_apy=_to_decimal(r.get("base_apy")),
                apy=_to_decimal(r.get("total_apy")),
                tvl=_to_decimal(r.get("tvl_val")),
                volume=None,
                tx=None,
                price=price,
                created_at=r.get("created_at"),
            )
            out.append(rec)
        return out

    # ---------- Insert ----------
    def insert_full_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        cur = self.cursor()
        sql = """
            INSERT INTO full_table
            (source, chain, batch_id, symbol, farm_apy, pool_apy, apy, tvl, volume, tx, price, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (
                r["source"],
                r["chain"],
                r["batch_id"],
                r["symbol"],
                r["farm_apy"],
                r["pool_apy"],
                r["apy"],
                r["tvl"],
                r["volume"],
                r["tx"],
                r["price"],
                r["created_at"],
            )
            for r in rows
        ]
        cur.executemany(sql, data)
        self.conn.commit()
        cur.close()
        return len(rows)

    # ---------- Main ----------
    def run_once(self) -> None:
        self.ensure_full_table()

        hydration_batch = self.latest_batch_id("hydration_data")
        pool_batch = self.latest_batch_id("pool_data")

        # Build latest price map from Hydration + (fallback) Bifrost staking (latest per symbol)
        price_map = self.latest_price_map()
        price_batch = getattr(self, "_latest_price_batch", None)

        rows: List[Dict[str, Any]] = []
        if hydration_batch is not None:
            rows.extend(self.rows_from_hydration(hydration_batch, price_map))
        if pool_batch is not None:
            rows.extend(self.rows_from_pool(pool_batch, price_map))

        # Always use Bifrost_site_table latest-per-asset for APY
        rows.extend(self.rows_from_bifrost_site_latest(price_map))

        inserted = self.insert_full_rows(rows)
        logger.info(f"Inserted {inserted} row(s) into full_table from latest sources.")
        if hydration_batch is not None:
            logger.info(f"  - hydration_data batch_id = {hydration_batch}")
        if pool_batch is not None:
            logger.info(f"  - pool_data batch_id      = {pool_batch}")
        logger.info("  - bifrost source          = Bifrost_site_table (latest-per-asset, APY)")
        if price_batch is not None:
            logger.info(f"  - Hydration_price batch_id = {price_batch} (prices primary)")
        logger.info("  - Bifrost_staking_table used as price fallback (latest per symbol)")

def main() -> None:
    load_dotenv()
    user = os.getenv("DB_USERNAME", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    db = os.getenv("DB_NAME", "quantDATA")
    combiner = SQL_DB_CombinedTables(user=user, password=password, db=db, host=host)
    combiner.run_once()

if __name__ == "__main__":
    main()

