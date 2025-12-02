import { ApiPromise, WsProvider } from '@polkadot/api';
import { TradeRouter, PoolService, EvmClient } from '@galacticcouncil/sdk';

// 1) Force any accidental console.log to go to stderr
// const origLog = console.log;
// console.log = (...args: any[]) => process.stderr.write(args.join(' ') + '\n');

// (keep console.warn / console.error as they already go to stderr)

async function getBatchPrices() {
  const wsProvider = new WsProvider('wss://hydration-rpc.n.dwellir.com'); // WSS endpoint
  const api = await ApiPromise.create({ provider: wsProvider });

  const poolService = new PoolService(api, new EvmClient(api));
  await poolService.syncRegistry();
  const tradeRouter = new TradeRouter(poolService);

  const assetIds = Array.from({ length: 31 }, (_, i) => i.toString()); // 0..30
  const results: { assetId: string; price: number }[] = [];
  const usdtId = '10'; // USDT (6 decimals)

  for (const assetId of assetIds) {
    if (assetId === usdtId) {
      console.error(`Skipping ${assetId} (USDT). Price=1`);
      results.push({ assetId, price: 1 });
      continue;
    }
    try {
      const trade = await tradeRouter.getBestSell(assetId, usdtId, '1'); // sell 1 unit -> USDT
      const priceRaw = trade.amountOut.toNumber();
      const price = priceRaw / 1_000_000; // USDT has 6 decimals
      if (price > 0) results.push({ assetId, price });
      else console.error(`Invalid price for ${assetId}: ${price}`);
    } catch (e: any) {
      console.error(`No path for ${assetId}: ${e?.message || e}`);
    }
  }

  poolService.destroy()
  await api.disconnect();

  // 2) Print ONLY JSON to stdout (use process.stdout explicitly)
  process.stdout.write(JSON.stringify(results) + '\n');
}

getBatchPrices().then(()=>{
  process.exit(0)
}).catch(err => {
  // Send errors to stderr as JSON (still not polluting stdout)
  // process.stderr.write(JSON.stringify({ error: String(err?.message || err) }) + '\n');
  // process.exit(1);
});

