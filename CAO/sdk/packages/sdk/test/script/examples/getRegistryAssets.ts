import { ApiPromise } from '@polkadot/api';
import { AssetClient } from '@galacticcouncil/sdk';
import { writeFileSync } from 'fs';
import { PolkadotExecutor } from '../PjsExecutor';
import { ApiUrl } from '../types';

import externalDegen from '../config/external.degen.json';
import external from '../config/external.degen.json';

class GetAssetsExample extends PolkadotExecutor {
  async script(api: ApiPromise): Promise<any> {
    const assetClient = new AssetClient(api);
    const assets = await assetClient.getOnChainAssets(true, external);

    // Process assets into CSV string
    const header = 'ID,Symbol\n';
    const rows = assets
      .map((asset: any) => `${asset.id},${asset.symbol ?? 'N/A'}`)
      .join('\n');
    const csvContent = header + rows;

    // Write to file
    try {
      writeFileSync('./allAssets.csv', csvContent, 'utf8');
      console.log('Assets successfully written to allAssets.csv');
    } catch (error) {
      console.error('Error writing to CSV:', error);
    }

    return assets; // Optional: return original data
  }
}

new GetAssetsExample(ApiUrl.HydraDx, 'Get all assets').run();
