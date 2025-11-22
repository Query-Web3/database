// sdk/packages/sdk/test/script/examples/getBatchPrice.ts
import { ApiPromise, WsProvider } from '@polkadot/api';
import { TradeRouter, PoolService } from '@galacticcouncil/sdk';

async function getBatchPrices() {
  const wsProvider = new WsProvider('wss://rpc.hydradx.cloud');
  const api = await ApiPromise.create({ provider: wsProvider });

  const poolService = new PoolService(api);
  await poolService.syncRegistry();
  const tradeRouter = new TradeRouter(poolService);

  const assetIds = Array.from({ length: 31 }, (_, i) => i.toString()); // 0 to 30
  const results: { assetId: string; price: number }[] = [];
  const usdtId = '10'; // USDT asset ID

  for (const assetId of assetIds) {
    if (assetId === usdtId) {
      // Skip USDT-to-USDT trade and assume price is 1
      console.warn(`Warning: Skipping asset ${assetId} (USDT) as it is the target asset. Price set to 1 USDT.`);
      results.push({ assetId, price: 1 });
      continue;
    }

    try {
      // Sell 1 unit of assetId for USDT (10)
      const trade = await tradeRouter.getBestSell(assetId, usdtId, '1');
      const priceRaw = trade.amountOut.toNumber(); // Raw USDT amount
      const price = priceRaw / 1_000_000; // Adjust by dividing by 1,000,000
      if (price <= 0) {
        console.warn(`Warning: Invalid price (0 or negative) for asset ${assetId}. Skipping.`);
        continue;
      }
      results.push({ assetId, price });
    } catch (error) {
      console.warn(`Warning: No trade path found for asset ${assetId}: ${error.message}`);
      // Do not push to results, effectively skipping this asset
    }
  }

  await api.disconnect();
  // Output only valid results as JSON to stdout
  console.log(JSON.stringify(results));
}

getBatchPrices().catch(err => console.error(JSON.stringify({ error: err.message })));
