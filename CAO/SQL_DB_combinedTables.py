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

    # ---------- Latest price map from Hydration_price + Bifrost_staking_table ----------
    def latest_price_map(self) -> Dict[str, Decimal]:
        mp: Dict[str, Decimal] = {}

        # 1) Primary: Hydration_price (latest batch)
        hydr_batch = self.latest_batch_id("Hydration_price")
        if hydr_batch is not None:
            try:
                rows = self.execute(
                    """
                    SELECT symbol, price_usdt
                    FROM `Hydration_price`
                    WHERE batch_id = %s
                    """,
                    (hydr_batch,),
                )
                for r in rows:
                    sym = r.get("symbol")
                    price = _to_decimal(r.get("price_usdt"))
                    if sym is None or price is None:
                        continue
                    mp[str(sym).lower()] = price
                # keep for logging
                self._latest_price_batch = hydr_batch  # type: ignore[attr-defined]
            except MySQLError as e:
                print(f"⚠️ Hydration_price read failed for batch {hydr_batch}: {e}")

        # 2) Augment: Bifrost_staking_table (latest batch by batch_id)
        bifrost_batch = self.latest_batch_id("Bifrost_staking_table")
        if bifrost_batch is not None:
            try:
                # Use latest batch_id; ORDER BY created_at ensures most recent rows first if duplicates exist
                rows = self.execute(
                    """
                    SELECT symbol, price
                    FROM `Bifrost_staking_table`
                    WHERE batch_id = %s
                    ORDER BY created_at DESC
                    """,
                    (bifrost_batch,),
                )
                for r in rows:
                    sym = r.get("symbol")
                    price = _to_decimal(r.get("price"))
                    if sym is None or price is None:
                        continue
                    k = str(sym).lower()
                    # Only fill gaps; keep Hydration as source of truth when present
                    if k not in mp:
                        mp[k] = price
                # keep for logging
                self._latest_bifrost_price_batch = bifrost_batch  # type: ignore[attr-defined]
            except MySQLError as e:
                print(f"⚠️ Bifrost_staking_table read failed for batch {bifrost_batch}: {e}")

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
        # Discover actual columns (case-insensitive)
        cols_lower = set(self.table_columns_lower(table))

        def first_present(candidates):
            for c in candidates:
                if c.lower() in cols_lower:
                    return c  # return original candidate (proper case for quoting)
            return None

        # Real column names (if present)
        batch_col = first_present(["batch_id", "BatchID", "batch", "batchId"])
        sym_col   = first_present(["symbol", "Symbol", "asset", "Asset"])
        farm_col  = first_present(["farmingAPY", "apyReward", "farming_apy"])
        base_col  = first_present(["baseApy", "apyBase", "base_apy"])
        total_col = first_present(["totalApy", "apy", "total_apy"])
        tvl_col   = first_present(["tvl", "TVL", "tvl_usd"])
        time_col  = first_present(["created_at", "CreatedAt", "timestamp"])

        def pick(real_name: str | None, alias: str) -> str:
            return f"`{real_name}` AS {alias}" if real_name else f"NULL AS {alias}"

        select_parts = [
            pick(batch_col, "batch_id"),
            pick(sym_col,   "sym"),
            pick(farm_col,  "farming_apy"),
            pick(base_col,  "base_apy"),
            pick(total_col, "total_apy"),
            pick(tvl_col,   "tvl_val"),
            pick(time_col,  "created_at"),
        ]

        where_clauses = []
        params: List[Any] = []

        # Only filter by batch if the column exists
        if batch_col:
            where_clauses.append(f"`{batch_col}` = %s")
            params.append(batch_id)

        # Exclude meta rows like tvl/address/revenue if a symbol-like column exists
        if sym_col:
            # Case-insensitive NOT IN
            where_clauses.append(f"LOWER(`{sym_col}`) NOT IN (%s, %s, %s, %s)")
            params.extend(["tvl", "addresses", "revenue","bncPrice"])

        sql = f"SELECT {', '.join(select_parts)} FROM `{table}`"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        try:
            rows = self.execute(sql, tuple(params))
        except MySQLError as e:
            print(f"⚠️ MySQL error when querying {table}: {e}\nSQL: {sql}\nPARAMS: {params}")
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
