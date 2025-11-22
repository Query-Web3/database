import { ApiPromise } from '@polkadot/api';
import { FarmClient } from '@galacticcouncil/sdk';
import { PolkadotExecutor } from '../PjsExecutor';
import { ApiUrl } from '../types';
import * as fs from 'fs';
import * as path from 'path';

// Get output path from command line argument
const args = process.argv.slice(2);
const outputPathArg = args[0];

if (!outputPathArg) {
  console.error('❌ Please provide an output path as the first argument.\nUsage: npx tsx getFarmApr.ts ./output/farm_apr.json');
  process.exit(1);
}

class GetFarmAprExample extends PolkadotExecutor {
  private outputPath: string;

  constructor(apiUrl: string, name: string, outputPath: string) {
    super(apiUrl, name);
    this.outputPath = outputPath;
  }

  async script(api: ApiPromise): Promise<any> {
    const farmClient = new FarmClient(api);
    const results: Record<string, any> = {};

    for (let id = 1; id <= 35; id++) {
      try {
        const apr = await farmClient.getFarmApr(id.toString(), 'omnipool');
        results[`${id}`] = apr;
      } catch (err) {
        results[`${id}`] = { error: err.message || err.toString() };
      }
    }

    // Write to file
    try {
      const resolvedPath = path.resolve(this.outputPath);
      fs.writeFileSync(resolvedPath, JSON.stringify(results, null, 2));
      console.log(`✅ Farm APR results saved to: ${resolvedPath}`);
    } catch (err) {
      console.error('❌ Failed to save results:', err);
    }

    return results;
  }
}

new GetFarmAprExample('wss://hydradx-rpc.dwellir.com', 'Get farm apr from 1 to 35', outputPathArg).run();

