Bifrost 数据存储自动化脚本说明

Bifrost_Data_fetching.py 脚本自动获取并存储数据到 MySQL 数据库表中,通过使用 batch_id 系统，可以高效地对每次运行的数据进行分组和检索。当前程序会每个小时更新一次数据并存入数据库，时间可以更新

功能特点
自动数据获取：脚本从 Bifrost API 获取数据，并将其处理后存储到两张表中：Bifrost_site_table 和 Bifrost_staking_table。
批次 ID 系统：每次运行都会基于当前 Unix 时间戳（int(time.time())）生成唯一的 batch_id，用于标记同一批次的数据。
每小时自动运行：脚本可配置为每小时运行一次，使用如 cron（Linux/macOS）或任务计划程序（Windows）。

运行环境要求
Python 环境：

确保安装了 Python 3.6 及以上版本。
安装所需的依赖库：
bash
Copy
Edit
pip install pandas mysql-connector-python requests
MySQL 数据库：

确保 MySQL 数据库已经设置好。
使用提供的表创建语句创建 Bifrost_site_table 和 Bifrost_staking_table。



如何使用？ 
直接执行脚本： python Bifrost_Data_fetching.py 




1. Bifrost_site_table
Description
The Bifrost_site_table stores comprehensive metrics and analytics about assets within the Bifrost ecosystem. This table is designed to capture a snapshot of various financial and performance-related data for each batch.

Schema
Column	Type	Description
auto_id	INT AUTO_INCREMENT	Unique identifier for each record.
batch_id	INT NOT NULL	Identifier to group records processed in the same batch.
Asset	VARCHAR(255)	Name of the asset.
Value	DECIMAL(20,3)	The value of the asset.
tvl	DECIMAL(20,6)	Total value locked.
tvm	DECIMAL(20,6)	Total value minted.
holders	INT	Number of holders of the asset.
apy	DECIMAL(20,6)	Annual percentage yield.
apyBase	DECIMAL(20,6)	Base APY.
apyReward	DECIMAL(20,6)	Reward APY.
totalIssuance	DECIMAL(20,6)	Total issuance of the asset.
holdersList	TEXT	List of holders, typically in JSON format.
annualized_income	DECIMAL(20,6)	Annualized income from the asset.
bifrost_staking_7day_apy	DECIMAL(20,6)	APY for Bifrost staking over the past 7 days.
created	DATETIME	Timestamp when the data was created.
daily_reward	DECIMAL(20,6)	Daily reward from the asset.
exited_node	INT	Number of nodes that have exited.
exited_not_transferred_node	INT	Nodes exited but not transferred.
exiting_online_node	INT	Online nodes exiting the network.
gas_fee_income	DECIMAL(20,6)	Income generated from gas fees.
id	INT	Identifier for the asset or node.
mev_7day_apy	DECIMAL(20,6)	APY from miner extractable value (MEV) over the last 7 days.
mev_apy	DECIMAL(20,6)	APY from MEV.
mev_income	DECIMAL(20,6)	Income from MEV activities.
online_node	INT	Number of nodes currently online.
slash_balance	DECIMAL(20,6)	Amount of balance slashed.
slash_num	INT	Number of slashing events.
staking_apy	DECIMAL(20,6)	APY from staking.
staking_income	DECIMAL(20,6)	Income generated from staking.
total_apy	DECIMAL(20,6)	Total APY.
total_balance	DECIMAL(20,6)	Total balance of the asset.
total_effective_balance	DECIMAL(20,6)	Total effective balance of the asset.
total_node	INT	Total number of nodes.
total_reward	DECIMAL(20,6)	Total rewards distributed.
total_withdrawals	DECIMAL(20,6)	Total withdrawals.
stakingApy	DECIMAL(20,6)	Staking APY.
stakingIncome	DECIMAL(20,6)	Staking income.
mevApy	DECIMAL(20,6)	MEV APY.
mevIncome	DECIMAL(20,6)	MEV income.
gasFeeApy	DECIMAL(20,6)	Gas fee APY.
gasFeeIncome	DECIMAL(20,6)	Gas fee income.
totalApy	DECIMAL(20,6)	Total APY across activities.
totalIncome	DECIMAL(20,6)	Total income across activities.
baseApy	DECIMAL(20,6)	Base APY.
farmingAPY	DECIMAL(20,6)	Farming APY.
veth2TVS	DECIMAL(20,6)	Value of Ethereum 2.0 staking.
apyMev	DECIMAL(20,6)	APY from MEV.
apyGas	DECIMAL(20,6)	APY from gas-related activities.
created_at	TIMESTAMP	Timestamp of record creation in the database.
2. Bifrost_staking_table
Description
The Bifrost_staking_table stores data about staking activities, including staking details for specific assets.

Schema
Column	Type	Description
id	INT AUTO_INCREMENT	Unique identifier for each record.
batch_id	INT NOT NULL	Identifier to group records processed in the same batch.
contractAddress	VARCHAR(255)	Contract address of the staking asset.
symbol	VARCHAR(50)	Symbol of the staking asset.
slug	VARCHAR(100)	Slug or identifier for the asset.
baseSlug	VARCHAR(100)	Base slug identifier.
unstakingTime	INT	Time required for unstaking.
users	INT	Number of users staking the asset.
apr	DECIMAL(20,6)	Annual percentage return.
fee	DECIMAL(20,6)	Fee associated with staking.
price	DECIMAL(20,6)	Price of the staking asset.
exchangeRatio	DECIMAL(20,6)	Exchange ratio for the asset.
supply	DECIMAL(20,6)	Supply of the staking asset.
created_at	TIMESTAMP	Timestamp of record creation in the database.


