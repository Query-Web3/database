// Node 18+
// npm i @polkadot/api @galacticcouncil/sdk p-limit
import { ApiPromise, WsProvider } from "@polkadot/api";
import { PoolService, TradeRouter } from "@galacticcouncil/sdk";
import pLimit from "p-limit";

const WSS       = process.env.HDX_RPC || "wss://rpc.hydradx.cloud:443";
const FORCE_STABLE_ID = process.env.HDX_STABLE_ID || ""; // e.g. "10" if you know USDT id
const CONCURRENCY = Number(process.env.HDX_CONCURRENCY || "6"); // avoid overloading WS
const ARG_IDS = process.argv.slice(2); // pass asset IDs; if empty, script will auto-list Omnipool IDs

const log = (...a) => process.stderr.write(a.join(" ") + "\n");

// --- chain helpers ----------------------------------------------------

async function getAssetDetails(api, id) {
  const opt = await api.query.assetRegistry.assets(id);
  return opt?.isSome ? opt.unwrap() : null;
}
async function getSymbol(api, id) {
  const det = await getAssetDetails(api, id);
  const sym = det?.symbol?.toHuman?.() ?? det?.symbol ?? det?.ticker ?? "";
  return typeof sym === "string" ? sym : (sym?.toString?.() ?? String(id));
}
async function getDecimals(api, id) {
  const det = await getAssetDetails(api, id);
  const d = Number(det?.decimals?.toString?.() ?? det?.decimals ?? 12);
  return Number.isFinite(d) ? d : 12;
}
function firstNumber(obj, keys) {
  for (const k of keys) {
    const v = obj?.[k];
    if (v != null) {
      const n = Number(v.toString?.() ?? v);
      if (Number.isFinite(n)) return n;
    }
  }
  return null;
}
async function getOmnipoolReserveRaw(api, id) {
  if (!api.query.omnipool?.assets) return null;
  const opt = await api.query.omnipool.assets(id);
  if (!opt || opt.isNone) return null;
  const st = opt.unwrap();
  // pallet field names vary by runtime; try common variants:
  const n = firstNumber(st, ["reserve", "liquidity", "balance", "amount", "totalLiquidity"]);
  return n != null ? String(n) : null;
}

// --- stable detection & pricing --------------------------------------

async function discoverStableId(api, omnipoolIds) {
  if (FORCE_STABLE_ID) return FORCE_STABLE_ID;
  const stableCandidates = new Set(["USDT","USDt","USDC","DAI"]);
  // try all omnipool ids first (fastest), then a few common registry ids
  const extras = ["10","11","12","100","101","102","103"];
  for (const id of [...omnipoolIds, ...extras]) {
    try {
      const sym = (await getSymbol(api, id)).toUpperCase();
      if (stableCandidates.has(sym)) return id;
    } catch {}
  }
  return ""; // none found (router may still pathfind via stableswap)
}

async function priceToStable(poolService, router, api, assetId, stableId, decimalsCache) {
  // If asset is the stable, price = 1
  const sym = (await getSymbol(api, assetId)).toUpperCase();
  if (["USDT","USDt","USDC","DAI"].includes(sym)) return 1;

  // Route 1 unit of asset -> stable
  try {
    const trade = await router.getBestSell(String(assetId), String(stableId), "1");
    // determine stable decimals once (cache by id)
    if (!decimalsCache.has(stableId))
      decimalsCache.set(stableId, await getDecimals(api, stableId));
    const d = decimalsCache.get(stableId);
    const out = Number(trade.amountOut.toString());
    return out / Math.pow(10, d);
  } catch (e) {
    log("price route error", assetId, "->", stableId, e?.message || String(e));
    return 0;
  }
}

// --- main -------------------------------------------------------------

(async () => {
  const api = await ApiPromise.create({ provider: new WsProvider(WSS) });

  // enumerate Omnipool asset IDs if none passed
  let ids = ARG_IDS;
  if (!ids.length) {
    const keys = await api.query.omnipool.assets.keys();
    ids = keys.map(k => k.args[0].toString());
  }

  // build services once
  const poolService = new PoolService(api);
  await poolService.syncRegistry();
  const router = new TradeRouter(poolService);

  // discover a stable to quote in USD terms (USDT/USDC/DAI)
  const stableId = await discoverStableId(api, ids);
  if (!stableId) log("warning: no direct stable found; router may still pathfind via stableswap");

  const decimalsCache = new Map();
  const limit = pLimit(CONCURRENCY);
  const tasks = ids.map((id) => limit(async () => {
    try {
      const raw = await getOmnipoolReserveRaw(api, id);
      if (!raw) return null; // not in omnipool or zero reserve
      if (!decimalsCache.has(id)) decimalsCache.set(id, await getDecimals(api, id));
      const dec = decimalsCache.get(id);
      const amount = Number(raw) / Math.pow(10, dec);
      if (!Number.isFinite(amount) || amount <= 0) return null;
      const pxUsd = stableId ? await priceToStable(poolService, router, api, id, stableId, decimalsCache) : 0;
      const tvlUsd = amount * (pxUsd || 0);
      return { assetId: String(id), tvl_usd: tvlUsd };
    } catch (e) {
      log("asset error", id, e?.message || String(e));
      return null;
    }
  }));

  const rows = (await Promise.all(tasks)).filter(Boolean);
  await api.disconnect();
  // stdout: pure JSON
  console.log(JSON.stringify(rows));
})().catch(e => { log("fatal", e?.message || String(e)); process.exit(1); });

