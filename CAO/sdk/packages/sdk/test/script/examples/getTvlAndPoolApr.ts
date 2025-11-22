// scripts/getTvlAndPoolApr.ts
import { ApiPromise, WsProvider } from "@polkadot/api";
import { PoolService, TradeRouter } from "@galacticcouncil/sdk";

/**
 * Output format:
 * [
 *   { assetId: "0", tvlUsd: 12345.67, poolAprPerc: 0.0 },
 *   ...
 * ]
 *
 * Notes:
 * - TVL = (omnipool reserve of asset) * (price in USDT)
 * - Price is estimated via best sell quote of "1 unit" into USDT (id = 10).
 * - poolAprPerc is left at 0 (placeholder), since fee-indexing requires an indexer.
 *   If you later add a fee source, update `poolAprPerc` accordingly.
 */

const WS = process.env.HYDRATION_WS || "wss://rpc.hydradx.cloud";
const USDT_ID = "10";

async function main() {
  // Allow asset ids from CLI, e.g.: `npx tsx scripts/getTvlAndPoolApr.ts 0,1,2,3`
  const arg = process.argv[2] || "";
  const assetIds = arg
    ? arg.split(",").map((s) => s.trim()).filter(Boolean)
    : Array.from({ length: 64 }, (_, i) => String(i)); // sensible default span

  const ws = new WsProvider(WS);
  const api = await ApiPromise.create({ provider: ws });

  const poolService = new PoolService(api);
  await poolService.syncRegistry();           // load pools, registry, reserves
  const tradeRouter = new TradeRouter(poolService);

  const results: { assetId: string; tvlUsd: number; poolAprPerc: number }[] = [];

  for (const assetId of assetIds) {
    try {
      // 1) reserve in the Omnipool for this asset (in base units)
      const state = poolService.getOmnipoolAssetState(assetId);
      if (!state) continue; // asset not in omnipool

      const { hubReserve, balance, decimals } = state; 
      // Prefer `balance` (asset reserve). Some SDK versions expose `balance`
      // or `reserve`. If you only have `balance`, use it; if you only have
      // `reserve`, rename accordingly.

      const reserve = balance ?? state.reserve ?? "0";
      const reserveFloat =
        typeof reserve === "string" ? Number(reserve) : Number(reserve);

      if (!isFinite(reserveFloat) || reserveFloat <= 0) continue;

      // 2) price in USDT (sell 1 unit -> USDT). Skip USDT->USDT.
      let priceUsd = 0;
      if (assetId === USDT_ID) {
        priceUsd = 1;
      } else {
        try {
          // amount "1" in human units; router accepts human strings
          const trade = await tradeRouter.getBestSell(assetId, USDT_ID, "1");
          // amountOut is in USDT base units (6 decimals typically)
          const usdtOut = trade.amountOut.toNumber() / 1_000_000;
          if (usdtOut > 0) priceUsd = usdtOut;
        } catch {
          // no direct path; skip asset
          continue;
        }
      }

      if (priceUsd <= 0) continue;

      // 3) TVL = reserve(human) * price
      const humanReserve = reserveFloat / Math.pow(10, decimals ?? 12);
      const tvlUsd = humanReserve * priceUsd;

      // 4) Pool APR via fees requires an indexer (not available here). Keep 0.
      const poolAprPerc = 0;

      results.push({ assetId, tvlUsd, poolAprPerc });
    } catch {
      // skip problematic assets without breaking the whole run
      continue;
    }
  }

  await api.disconnect();

  // Print JSON only — no extra logs
  process.stdout.write(JSON.stringify(results));
}

main().catch((err) => {
  // print only JSON error
  process.stdout.write(JSON.stringify({ error: String(err?.message || err) }));
  process.exit(1);
});

