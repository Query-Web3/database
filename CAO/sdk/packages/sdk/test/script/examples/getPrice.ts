// sdk/packages/sdk/test/script/examples/getPrice.ts
import { ApiPromise, WsProvider } from '@polkadot/api';
import { TradeRouter, PoolService } from '@galacticcouncil/sdk';

async function getPrice(assetId: string) {
  const wsProvider = new WsProvider('wss://rpc.hydradx.cloud');
  const api = await ApiPromise.create({ provider: wsProvider });

  const poolService = new PoolService(api);
  await poolService.syncRegistry();
  const tradeRouter = new TradeRouter(poolService);

  // Sell 1 unit of assetId for USDT (10)
  const trade = await tradeRouter.getBestSell(assetId, '10', '1');
  const price = trade.amountOut.toNumber(); // USDT amount received for 1 unit of asset

  await api.disconnect();
  return { assetId, price };
}

const assetId = process.argv[2]; // Get asset ID from command line
getPrice(assetId)
  .then(result => console.log(JSON.stringify(result)))
  .catch(err => console.error(JSON.stringify({ error: err.message })));
