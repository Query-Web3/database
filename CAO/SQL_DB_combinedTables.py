\
#!/usr/bin/env python3
# SQL_DB_combinedTables.py
"""
Combine latest batches from hydration_data, pool_data, and Bifrost tables
into a unified append-only table: full_table.

Adds columns:
  - `chain`  : 'hydration' | 'moonbeam' | 'bifrost'
  - `price`  : matched from latest Hydration_price (by symbol), else NULL

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


Decimal = decimal.Decimal


def _to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float, str)):
            s = str(v).strip()
            if s == "" or s.lower() == "nan":
                return None
            return Decimal(s)
    except Exception:
        return None
    return None


class SQL_DB_CombinedTables:
    def __init__(self, user: str, password: str, db: str, host: str = "127.0.0.1") -> None:
        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.conn = None  # type: ignore

    # ---------- DB helpers ----------
    def connect(self) -> None:
        if self.conn:
            return
        self.conn = mysql.connector.connect(
            user=self.user, password=self.password, host=self.host, database=self.db
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
        # Create with chain & price columns if missing; if table exists without them, try to alter.
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
        # Try to add the columns if this script runs against an older table version
        try:
            self.execute("ALTER TABLE full_table ADD COLUMN chain VARCHAR(32) NOT NULL DEFAULT 'hydration'")
        except MySQLError:
            pass
        try:
            self.execute("ALTER TABLE full_table ADD COLUMN price DECIMAL(40,18) NULL")
        except MySQLError:
            pass

    # ---------- Utility: existing columns ----------
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

    # ---------- Fetch latest batch per table ----------
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

    # ---------- Latest price map from Hydration_price ----------
    def latest_price_map(self) -> Dict[str, Decimal]:
        batch = self.latest_batch_id("Hydration_price")
        if batch is None:
            return {}
        rows = self.execute(
            """
            SELECT symbol, price_usdt
            FROM `Hydration_price`
            WHERE batch_id = %s
            """,
            (batch,),
        )
        mp: Dict[str, Decimal] = {}
        for r in rows:
            sym = r.get("symbol")
            if sym is None:
                continue
            price = _to_decimal(r.get("price_usdt"))
            if price is None:
                continue
            mp[str(sym).lower()] = price
        # Keep for logging
        self._latest_price_batch = batch  # type: ignore
        return mp

    # ---------- Extractors (only needed columns) ----------
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
            # Single price is ambiguous for pools; set NULL for now.
            rec = dict(
                source="pool_data",
                chain="moonbeam",
                batch_id=r.get("batch_id"),
                symbol=json.dumps(sym),
                farm_apy=_to_decimal(r.get("farming_apr")),
                pool_apy=_to_decimal(r.get("pools_apr")),
                apy=_to_decimal(r.get("final_apr")),
                tvl=None,  # not defined yet
                volume=_to_decimal(r.get("volume_usd_24h")),
                tx=(int(r["tx_count"]) if r.get("tx_count") is not None else None),
                price=None,
                created_at=r.get("created_at"),
            )
            out.append(rec)
        return out

    def rows_from_bifrost(self, batch_id: int, table: str, price_map: Dict[str, Decimal]) -> List[Dict[str, Any]]:
        # Build a safe SELECT that only references columns that actually exist.
        cols = set(self.table_columns_lower(table))

        def pick(candidates, alias):
            for c in candidates:
                if c.lower() in cols:
                    return f"`{c}` AS {alias}"
            return f"NULL AS {alias}"

        select_parts = [
            pick(["batch_id"], "batch_id"),
            pick(["symbol", "Symbol", "asset", "Asset"], "sym"),
            pick(["farmingAPY", "apyReward", "farming_apy"], "farming_apy"),
            pick(["baseApy", "apyBase", "base_apy"], "base_apy"),
            pick(["totalApy", "apy", "total_apy"], "total_apy"),
            pick(["tvl", "TVL", "tvl_usd"], "tvl_val"),
            pick(["created_at", "CreatedAt", "timestamp"], "created_at"),
        ]
        sql = f"SELECT {', '.join(select_parts)} FROM `{table}` WHERE batch_id = %s"

        try:
            rows = self.execute(sql, (batch_id,))
        except MySQLError as e:
            print(f"⚠️ MySQL error when querying {table}: {e}\\nSQL: {sql}")
            return []

        out: List[Dict[str, Any]] = []
        for r in rows:
            sym = r.get("sym")
            price = price_map.get(str(sym).lower()) if sym is not None else None
            rec = dict(
                source=table,
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
        bifrost_tables_to_try = ["Bifrost_site_table", "Bifrost_staking_table"]
        bifrost_batch = None
        bifrost_table_used = None
        for t in bifrost_tables_to_try:
            try:
                b = self.latest_batch_id(t)
            except MySQLError:
                b = None
            if b is not None:
                bifrost_batch = b
                bifrost_table_used = t
                break

        # Build latest price map (from Hydration_price)
        price_map = self.latest_price_map()
        price_batch = getattr(self, "_latest_price_batch", None)

        rows: List[Dict[str, Any]] = []
        if hydration_batch is not None:
            rows.extend(self.rows_from_hydration(hydration_batch, price_map))
        if pool_batch is not None:
            rows.extend(self.rows_from_pool(pool_batch, price_map))
        if bifrost_batch is not None and bifrost_table_used is not None:
            rows.extend(self.rows_from_bifrost(bifrost_batch, bifrost_table_used, price_map))

        inserted = self.insert_full_rows(rows)
        print(f"✅ Inserted {inserted} row(s) into full_table from latest batches.")
        if hydration_batch is not None:
            print(f"  - hydration_data batch_id = {hydration_batch}")
        if pool_batch is not None:
            print(f"  - pool_data batch_id      = {pool_batch}")
        if bifrost_batch is not None:
            print(f"  - {bifrost_table_used} batch_id = {bifrost_batch}")
        if price_batch is not None:
            print(f"  - Hydration_price batch_id = {price_batch}")


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
