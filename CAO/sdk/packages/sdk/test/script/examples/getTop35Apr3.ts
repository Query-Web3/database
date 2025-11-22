// getFarmApr.ts
// Usage:
//   npx tsx getFarmApr.ts ./output/farm_apr.json --from 1 --to 35 --rpc wss://hydradx-rpc.dwellir.com --concurrency 6 --timeout 20000

import { ApiPromise, WsProvider } from '@polkadot/api';
import { FarmClient } from '@galacticcouncil/sdk';
import * as fs from 'fs';
import * as path from 'path';

// ---------------------- CLI ----------------------
const args = process.argv.slice(2);
const outPath = args[0];

if (!outPath) {
  console.error('❌ Please provide an output path as the first argument.\n   Example: npx tsx getFarmApr.ts ./output/farm_apr.json');
  process.exit(1);
}

function pickFlag(name: string, fallback?: string) {
  const idx = args.findIndex(a => a === `--${name}`);
  if (idx >= 0 && args[idx + 1]) return args[idx + 1];
  return fallback;
}

const RPC_URL = pickFlag('rpc', 'wss://hydradx-rpc.dwellir.com')!;
const ID_FROM = parseInt(pickFlag('from', '1')!, 10);
const ID_TO = parseInt(pickFlag('to', '35')!, 10);
const CONCURRENCY = Math.max(1, parseInt(pickFlag('concurrency', '6')!, 10)); // 5-8 is a good range
const PER_CALL_TIMEOUT_MS = Math.max(1_000, parseInt(pickFlag('timeout', '20000')!, 10)); // 20s default

// ------------------ Small utils ------------------
function sleep(ms: number) {
  return new Promise(res => setTimeout(res, ms));
}

async function withTimeout<T>(p: Promise<T>, ms: number, tag: string): Promise<T> {
  let t: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, rej) => {
    t = setTimeout(() => rej(new Error(`Timeout after ${ms}ms (${tag})`)), ms);
  });
  try {
    const out = await Promise.race([p, timeout]);
    return out as T;
  } finally {
    if (t) clearTimeout(t);
  }
}

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, i: number) => Promise<R>
): Promise<R[]> {
  const ret: R[] = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: limit }, async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) break;
      ret[idx] = await fn(items[idx], idx);
    }
  });
  await Promise.all(workers);
  return ret;
}

// ------------------ Main script ------------------
(async () => {
  let api: ApiPromise | undefined;
  const resolvedPath = path.resolve(outPath);

  // nicer exits
  process.on('unhandledRejection', (e: any) => {
    console.error('🔴 UnhandledRejection:', e?.message ?? e);
    process.exit(1);
  });
  process.on('SIGINT', async () => {
    console.log('\n🛑 SIGINT');
    try { if (api) await api.disconnect(); } catch {}
    process.exit(1);
  });

  try {
    console.log(`➡️  Connecting RPC: ${RPC_URL}`);
    const provider = new WsProvider(RPC_URL, 1024); // 1024 ms autoConnect delay (polite)
    api = await ApiPromise.create({ provider });
    await api.isReady;
    console.log('✅ API ready');

    const farmClient = new FarmClient(api);

    // Prepare ID list
    if (Number.isNaN(ID_FROM) || Number.isNaN(ID_TO) || ID_FROM > ID_TO) {
      throw new Error(`Invalid id range: from=${ID_FROM} to=${ID_TO}`);
    }
    const ids = Array.from({ length: ID_TO - ID_FROM + 1 }, (_, k) => String(ID_FROM + k));

    const results: Record<string, any> = {};

    // Fetch in parallel with bounded concurrency
    console.log(`➡️  Fetching APRs for IDs ${ID_FROM}..${ID_TO} with concurrency=${CONCURRENCY}, timeout=${PER_CALL_TIMEOUT_MS}ms`);
    await mapWithConcurrency(ids, CONCURRENCY, async (id) => {
      const tag = `getFarmApr(id=${id})`;
      try {
        const apr = await withTimeout(
          farmClient.getFarmApr(id, 'omnipool'),
          PER_CALL_TIMEOUT_MS,
          tag
        );
        results[id] = apr;
        // Optional: tiny delay to avoid burst limits
        await sleep(25);
      } catch (err: any) {
        const msg = err?.message ?? String(err);
        results[id] = { error: msg };
        // brief backoff on errors to be polite with the RPC node
        await sleep(100);
      }
    });

    // Write file
    fs.mkdirSync(path.dirname(resolvedPath), { recursive: true });
    fs.writeFileSync(resolvedPath, JSON.stringify(results, null, 2));
    console.log(`💾 Saved APR JSON → ${resolvedPath}`);

  } catch (err: any) {
    console.error('❌ Failed:', err?.message ?? err);
    process.exitCode = 1;
  } finally {
    // Clean shutdown: disconnect WS to let Node exit promptly
    try {
      if (api) {
        await api.disconnect();
        // give the ws a tick to close
        await sleep(50);
      }
    } catch {}
    // last-resort: ensure process ends even if some handle lingers
    process.exit(process.exitCode ?? 0);
  }
})();

