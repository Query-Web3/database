# 📊 DeFi Analytics Database – Table Documentation

This repository contains a set of **fact tables** that capture DeFi protocol metrics from multiple ecosystems (Bifrost, Hydration, StellaSwap), along with a **unified analytics table** that consolidates cross-chain data into a single schema for downstream analysis.

---

## 📐 Design Philosophy

- **Batch-based ingestion**  
  All raw tables use `batch_id` to group data collected in the same fetch cycle.

- **Append-only facts**  
  Tables store time-series snapshots rather than mutable state.

- **Protocol-specific → Unified analytics**  
  Raw tables preserve protocol semantics, while the unified table standardizes metrics for comparison.

---

## 1️⃣ Bifrost_site_table (Fact Table)

### Description
The `Bifrost_site_table` stores **comprehensive asset-level and node-level metrics** from the Bifrost ecosystem, including TVL, APY breakdowns, staking income, MEV income, and validator statistics.

### Grain
> One record per **asset (or node)** per **batch_id**

### Schema

| Column | Type | Description |
|------|------|-------------|
| auto_id | INT | Auto-increment record ID |
| batch_id | INT | Batch identifier |
| Asset | VARCHAR(255) | Asset name |
| Value | DECIMAL | Asset value |
| tvl | DECIMAL | Total value locked |
| tvm | DECIMAL | Total value minted |
| holders | INT | Number of holders |
| apy | DECIMAL | Annual percentage yield |
| apyBase | DECIMAL | Base APY |
| apyReward | DECIMAL | Reward APY |
| totalIssuance | DECIMAL | Total issuance |
| holdersList | TEXT | JSON list of holders |
| annualized_income | DECIMAL | Annualized income |
| bifrost_staking_7day_apy | DECIMAL | 7-day staking APY |
| daily_reward | DECIMAL | Daily rewards |
| online_node | INT | Online nodes |
| exited_node | INT | Exited nodes |
| slash_num | INT | Slashing events |
| staking_apy | DECIMAL | Staking APY |
| staking_income | DECIMAL | Staking income |
| mev_apy | DECIMAL | MEV APY |
| mev_income | DECIMAL | MEV income |
| gas_fee_income | DECIMAL | Gas fee income |
| total_apy | DECIMAL | Total APY |
| total_balance | DECIMAL | Total balance |
| total_reward | DECIMAL | Total rewards |
| created | DATETIME | Source timestamp |
| created_at | TIMESTAMP | Insert timestamp |

### Notes
- Contains **multiple APY naming conventions** from upstream sources.
- Intended as a **raw fact table**, not normalized.

---

## 2️⃣ Bifrost_staking_table (Fact Table)

### Description
Stores **staking-specific configuration and performance data** for Bifrost assets.

### Grain
> One record per **staking asset** per **batch_id**

### Schema

| Column | Type | Description |
|------|------|-------------|
| id | INT | Auto-increment ID |
| batch_id | INT | Batch identifier |
| contractAddress | VARCHAR(255) | Contract address |
| symbol | VARCHAR(50) | Asset symbol |
| slug | VARCHAR(100) | Asset slug |
| baseSlug | VARCHAR(100) | Base slug |
| unstakingTime | INT | Unstaking time |
| users | INT | Number of stakers |
| apr | DECIMAL | Annual percentage return |
| fee | DECIMAL | Staking fee |
| price | DECIMAL | Asset price |
| exchangeRatio | DECIMAL | Exchange ratio |
| supply | DECIMAL | Asset supply |
| created_at | TIMESTAMP | Insert timestamp |

---

## 3️⃣ hydration_data (Fact Table)

### Description
Stores APR, TVL, and trading volume metrics from the **Hydration protocol**.

### Grain
> One record per **asset** per **batch_id**

### Schema

| Column | Type | Description |
|------|------|-------------|
| id | INT | Auto-increment ID |
| batch_id | INT | Batch identifier |
| asset_id | VARCHAR(50) | Asset ID |
| symbol | VARCHAR(50) | Asset symbol |
| farm_apr | DOUBLE | Farming APR |
| pool_apr | DOUBLE | Pool APR |
| total_apr | DOUBLE | Combined APR |
| tvl_usd | DOUBLE | TVL (USD) |
| volume_usd | DOUBLE | Trading volume |
| timestamp | VARCHAR(50) | Source timestamp |
| created_at | TIMESTAMP | Insert timestamp |

---

## 4️⃣ pool_data (Fact Table – Liquidity Pools)

### Description
Stores **liquidity pool–level analytics** from StellaSwap.

### Grain
> One record per **pool** per **batch_id**

### Schema

| Column | Type | Description |
|------|------|-------------|
| id | INT | Auto-increment ID |
| batch_id | INT | Batch identifier |
| pool_id | VARCHAR(255) | Pool ID |
| token0_symbol | VARCHAR(50) | Token0 symbol |
| token1_symbol | VARCHAR(50) | Token1 symbol |
| liquidity | DOUBLE | Pool liquidity |
| volume_usd_24h | DOUBLE | 24h volume |
| fees_usd_24h | DOUBLE | 24h fees |
| pools_apr | DOUBLE | Pool APR |
| farming_apr | DOUBLE | Farming APR |
| final_apr | DOUBLE | Final APR |
| token_rewards | TEXT | Reward tokens (JSON) |
| timestamp | VARCHAR(50) | Source timestamp |
| created_at | TIMESTAMP | Insert timestamp |

---

## 5️⃣ full_table (Unified Analytics Table)

### Description
A **cross-chain, protocol-agnostic analytics table** that consolidates key DeFi metrics into a single schema.

### Grain
> One record per **asset or pool** per **batch_id** per **chain** per **source**

### Schema

| Column | Type | Description |
|------|------|-------------|
| id | BIGINT | Auto-increment ID |
| source | VARCHAR(64) | Data source (protocol) |
| chain | VARCHAR(32) | Blockchain network |
| batch_id | BIGINT | Batch identifier |
| symbol | JSON | Asset or pool symbols |
| farm_apy | DECIMAL | Farming APY |
| pool_apy | DECIMAL | Pool APY |
| apy | DECIMAL | Total APY |
| tvl | DECIMAL | Total value locked |
| volume | DECIMAL | Trading volume |
| tx | BIGINT | Transaction count |
| price | DECIMAL | Asset price |
| created_at | DATETIME | Source timestamp |
| inserted_at | TIMESTAMP | Insert timestamp |

### Notes
- Derived table (not raw).
- Metric availability depends on `source`.
- Optimized for **dashboards, ranking, and cross-chain analysis**.

---

## 🔁 Data Flow
Bifrost_site_table ─┐
Bifrost_staking_table ─┤
hydration_data ────────┼──► full_table
pool_data ─────────────┘



