import { config } from 'dotenv';
import { DataSource } from 'typeorm';
import {
  User,
  UserBrandVotes,
  Brand,
  Category,
  Tag,
  BrandTags,
  UserDailyActions,
  AirdropScore,
  AirdropSnapshot,
  AirdropLeaf,
  RewardClaim,
} from '../src/models';
import { getConfig } from '../src/security/config';

config();

type BackfillStats = {
  checked: number;
  updated: number;
  skipped: number;
  failed: number;
  contractAddressUpdated: number;
  tickerUpdated: number;
  tickerTokenIdUpdated: number;
};

function normalizeMetadataHash(value: string | null | undefined): string {
  if (!value) return '';
  if (value.startsWith('ipfs://')) return value.slice(7);
  if (value.startsWith('/ipfs/')) return value.slice(6);
  return value;
}

function parseArgs(args: string[]): {
  dryRun: boolean;
  onlyOnChainId?: number;
} {
  const dryRun = !args.includes('--apply');

  const inlineOnly = args.find((arg) => arg.startsWith('--only-onchain-id='));
  const splitOnlyIndex = args.findIndex((arg) => arg === '--only-onchain-id');

  let onlyOnChainId: number | undefined;
  if (inlineOnly) {
    const raw = inlineOnly.split('=')[1];
    const parsed = Number(raw);
    if (Number.isFinite(parsed) && parsed > 0) {
      onlyOnChainId = parsed;
    }
  } else if (splitOnlyIndex >= 0 && args[splitOnlyIndex + 1]) {
    const parsed = Number(args[splitOnlyIndex + 1]);
    if (Number.isFinite(parsed) && parsed > 0) {
      onlyOnChainId = parsed;
    }
  }

  return { dryRun, onlyOnChainId };
}

async function fetchIpfsMetadata(metadataHash: string): Promise<any> {
  const gateways = [
    'https://ipfs.io/ipfs/',
    'https://cloudflare-ipfs.com/ipfs/',
    'https://gateway.pinata.cloud/ipfs/',
  ];

  for (const gateway of gateways) {
    const url = `${gateway}${metadataHash}`;
    try {
      const response = await fetch(url);
      if (!response.ok) continue;
      return await response.json();
    } catch {
      continue;
    }
  }

  throw new Error(`Failed to fetch IPFS metadata for hash: ${metadataHash}`);
}

async function run(): Promise<void> {
  const { dryRun, onlyOnChainId } = parseArgs(process.argv.slice(2));
  const appConfig = getConfig();

  console.log('🧩 Backfill Brand Token Fields');
  console.log(`Mode: ${dryRun ? 'DRY RUN' : 'APPLY'}`);
  if (onlyOnChainId) {
    console.log(`Filter: only onChainId=${onlyOnChainId}`);
  }

  const dataSource = new DataSource({
    type: 'mysql',
    host: appConfig.db.host,
    port: appConfig.db.port,
    username: appConfig.db.username,
    password: appConfig.db.password,
    database: appConfig.db.name,
    ssl: appConfig.db.requireSSL ? { rejectUnauthorized: false } : false,
    entities: [
      User,
      UserBrandVotes,
      Brand,
      Category,
      Tag,
      BrandTags,
      UserDailyActions,
      AirdropScore,
      AirdropSnapshot,
      AirdropLeaf,
      RewardClaim,
    ],
    synchronize: false,
    logging: false,
  });

  const stats: BackfillStats = {
    checked: 0,
    updated: 0,
    skipped: 0,
    failed: 0,
    contractAddressUpdated: 0,
    tickerUpdated: 0,
    tickerTokenIdUpdated: 0,
  };

  try {
    await dataSource.initialize();
    const brandRepo = dataSource.getRepository(Brand);

    const candidatesQb = brandRepo
      .createQueryBuilder('brand')
      .where("brand.metadataHash IS NOT NULL AND brand.metadataHash <> ''")
      .andWhere(
        "(" +
          "brand.ticker IS NULL OR brand.ticker = '' " +
          'OR brand.contractAddress IS NULL OR brand.contractAddress = \'\' ' +
          "OR brand.tickerTokenId IS NULL OR brand.tickerTokenId = ''" +
          ")",
      )
      .orderBy('brand.onChainId', 'ASC');

    if (onlyOnChainId) {
      candidatesQb.andWhere('brand.onChainId = :onChainId', {
        onChainId: onlyOnChainId,
      });
    }

    const candidates = await candidatesQb.getMany();
    console.log(`Candidates: ${candidates.length}`);

    for (const brand of candidates) {
      stats.checked++;
      const onChainId = brand.onChainId ?? brand.id;
      const metadataHash = normalizeMetadataHash(brand.metadataHash);

      if (!metadataHash) {
        stats.skipped++;
        continue;
      }

      try {
        const metadata = await fetchIpfsMetadata(metadataHash);
        const metadataContractAddress =
          metadata?.contractAddress ?? metadata?.tokenContractAddress;
        const metadataTicker = metadata?.ticker ?? metadata?.tokenTicker;
        const metadataTickerTokenId =
          typeof metadata?.tickerTokenId === 'string'
            ? metadata.tickerTokenId
            : undefined;

        const patch: Partial<Brand> = {};

        if (
          typeof metadataContractAddress === 'string' &&
          metadataContractAddress.trim() &&
          metadataContractAddress !== brand.contractAddress
        ) {
          patch.contractAddress = metadataContractAddress;
        }

        if (
          typeof metadataTicker === 'string' &&
          metadataTicker.trim() &&
          metadataTicker !== brand.ticker
        ) {
          patch.ticker = metadataTicker;
        }

        if (
          typeof metadataTickerTokenId === 'string' &&
          metadataTickerTokenId.trim() &&
          metadataTickerTokenId !== brand.tickerTokenId
        ) {
          patch.tickerTokenId = metadataTickerTokenId;
        }

        if (Object.keys(patch).length === 0) {
          stats.skipped++;
          console.log(
            `⏭️  onChainId=${onChainId} no token updates found in metadata`,
          );
          continue;
        }

        if (dryRun) {
          console.log(
            `🧪 [DRY] onChainId=${onChainId} patch=${JSON.stringify(patch)}`,
          );
        } else {
          await brandRepo.update({ id: brand.id }, patch);
          console.log(
            `✅ onChainId=${onChainId} updated fields=${Object.keys(patch).join(',')}`,
          );
        }

        if (patch.contractAddress !== undefined) {
          stats.contractAddressUpdated++;
        }
        if (patch.ticker !== undefined) {
          stats.tickerUpdated++;
        }
        if (patch.tickerTokenId !== undefined) {
          stats.tickerTokenIdUpdated++;
        }

        stats.updated++;
      } catch (error) {
        stats.failed++;
        const message = error instanceof Error ? error.message : String(error);
        console.error(`❌ onChainId=${onChainId} failed: ${message}`);
      }
    }

    console.log('\n📊 Backfill Summary');
    console.log(`checked: ${stats.checked}`);
    console.log(`updated: ${stats.updated}`);
    console.log(`skipped: ${stats.skipped}`);
    console.log(`failed: ${stats.failed}`);
    console.log(`contractAddressUpdated: ${stats.contractAddressUpdated}`);
    console.log(`tickerUpdated: ${stats.tickerUpdated}`);
    console.log(`tickerTokenIdUpdated: ${stats.tickerTokenIdUpdated}`);
  } finally {
    if (dataSource.isInitialized) {
      await dataSource.destroy();
    }
  }
}

run().catch((error) => {
  console.error('Fatal backfill error:', error);
  process.exit(1);
});
