#!/usr/bin/env bun

/**
 * Indexer Sync Script
 *
 * Syncs data from the PostgreSQL indexer database (source of truth)
 * to the MySQL production database.
 *
 * Syncs:
 * - Brands (new and updated)
 * - User brndPowerLevel
 * - Votes (missing ones)
 *
 * Usage:
 *   bun run scripts/sync-from-indexer.ts
 *
 * Environment variables required:
 *   - INDEXER_DB_URL: PostgreSQL connection string
 *   - INDEXER_DB_SCHEMA: Schema name (default: public)
 *   - DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME: MySQL connection
 */

import { Client } from 'pg';
import * as mysql from 'mysql2/promise';
import * as readline from 'readline';

// ============================================================================
// Types
// ============================================================================

interface SyncStats {
  brandsChecked: number;
  brandsCreated: number;
  brandsUpdated: number;
  usersChecked: number;
  usersUpdated: number;
  votesChecked: number;
  votesInserted: number;
  errors: string[];
  startTime: Date;
  endTime?: Date;
}

interface SyncOptions {
  windowHours: number;
  syncBrands: boolean;
  syncPowerLevels: boolean;
  syncVotes: boolean;
  dryRun: boolean;
}

// ============================================================================
// Prompts
// ============================================================================

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

function prompt(question: string): Promise<string> {
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      resolve(answer);
    });
  });
}

async function promptForOptions(): Promise<SyncOptions> {
  console.log('\n🔄 INDEXER SYNC SCRIPT');
  console.log('='.repeat(50));
  console.log('\nThis script syncs data from the PostgreSQL indexer');
  console.log('to the MySQL production database.\n');

  // Time window selection
  console.log('Select sync time window:');
  console.log('  1. Last 48 hours (recommended for daily sync)');
  console.log('  2. Last 7 days');
  console.log('  3. Last 30 days');
  console.log('  4. Full sync (all data)');
  console.log('  5. Custom (enter hours)');
  console.log('');

  const windowChoice = await prompt('Enter choice (1-5): ');

  let windowHours: number;
  switch (windowChoice.trim()) {
    case '1':
      windowHours = 48;
      break;
    case '2':
      windowHours = 168; // 7 * 24
      break;
    case '3':
      windowHours = 720; // 30 * 24
      break;
    case '4':
      windowHours = 0; // Full sync
      break;
    case '5':
      const customHours = await prompt('Enter number of hours: ');
      windowHours = parseInt(customHours, 10) || 48;
      break;
    default:
      console.log('Invalid choice, defaulting to 48 hours');
      windowHours = 48;
  }

  // What to sync
  console.log('\nWhat do you want to sync?');
  console.log('  1. All (brands, power levels, votes) (recommended)');
  console.log('  2. Only brands');
  console.log('  3. Only power levels');
  console.log('  4. Only votes');
  console.log('  5. Brands and votes (skip power levels)');
  console.log('');

  const syncChoice = await prompt('Enter choice (1-5): ');

  let syncBrands = true;
  let syncPowerLevels = true;
  let syncVotes = true;

  switch (syncChoice.trim()) {
    case '2':
      syncPowerLevels = false;
      syncVotes = false;
      break;
    case '3':
      syncBrands = false;
      syncVotes = false;
      break;
    case '4':
      syncBrands = false;
      syncPowerLevels = false;
      break;
    case '5':
      syncPowerLevels = false;
      break;
    default:
      // Default to all
      break;
  }

  // Dry run option
  console.log('\nRun mode:');
  console.log('  1. Live (actually sync data)');
  console.log('  2. Dry run (preview only, no changes)');
  console.log('');

  const modeChoice = await prompt('Enter choice (1-2): ');
  const dryRun = modeChoice.trim() === '2';

  // Confirm
  console.log('\n' + '='.repeat(50));
  console.log('SYNC CONFIGURATION:');
  console.log(`  Window: ${windowHours === 0 ? 'FULL SYNC' : `Last ${windowHours} hours`}`);
  console.log(`  Sync brands: ${syncBrands ? 'Yes' : 'No'}`);
  console.log(`  Sync power levels: ${syncPowerLevels ? 'Yes' : 'No'}`);
  console.log(`  Sync votes: ${syncVotes ? 'Yes' : 'No'}`);
  console.log(`  Mode: ${dryRun ? '🔍 DRY RUN (no changes)' : '🚀 LIVE'}`);
  console.log('='.repeat(50));

  const confirm = await prompt('\nProceed with sync? (y/n): ');
  if (confirm.toLowerCase() !== 'y') {
    console.log('Sync cancelled.');
    process.exit(0);
  }

  return { windowHours, syncBrands, syncPowerLevels, syncVotes, dryRun };
}

// ============================================================================
// Sync Logic
// ============================================================================

class IndexerSyncer {
  private pgClient: Client;
  private mysqlConn: mysql.Connection | null = null;
  private schema: string;
  private stats: SyncStats;

  constructor() {
    const connectionString = process.env.INDEXER_DB_URL;
    if (!connectionString) {
      throw new Error('INDEXER_DB_URL environment variable is required');
    }

    this.pgClient = new Client({ connectionString });
    this.schema = process.env.INDEXER_DB_SCHEMA || 'public';
    this.stats = {
      brandsChecked: 0,
      brandsCreated: 0,
      brandsUpdated: 0,
      usersChecked: 0,
      usersUpdated: 0,
      votesChecked: 0,
      votesInserted: 0,
      errors: [],
      startTime: new Date(),
    };
  }

  async connect(): Promise<void> {
    console.log('\n🔌 Connecting to databases...');

    // Connect to PostgreSQL
    await this.pgClient.connect();
    console.log('✅ Connected to PostgreSQL indexer');

    // Connect to MySQL
    const mysqlConfig = {
      host: process.env.DATABASE_HOST,
      port: parseInt(process.env.DATABASE_PORT || '3306', 10),
      user: process.env.DATABASE_USER,
      password: process.env.DATABASE_PASSWORD,
      database: process.env.DATABASE_NAME,
    };

    if (!mysqlConfig.host || !mysqlConfig.user || !mysqlConfig.database) {
      throw new Error(
        'MySQL environment variables required: DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME',
      );
    }

    this.mysqlConn = await mysql.createConnection(mysqlConfig);
    console.log('✅ Connected to MySQL production database');
  }

  async disconnect(): Promise<void> {
    console.log('\n🔌 Closing database connections...');
    await this.pgClient.end();
    if (this.mysqlConn) {
      await this.mysqlConn.end();
    }
    console.log('✅ Connections closed');
  }

  async sync(options: SyncOptions): Promise<SyncStats> {
    this.stats.startTime = new Date();

    if (options.dryRun) {
      console.log('\n🔍 DRY RUN MODE - No changes will be made\n');
    }

    try {
      // Sync brands FIRST so votes have valid brand references
      if (options.syncBrands) {
        await this.syncBrands(options.windowHours, options.dryRun);
      }

      if (options.syncPowerLevels) {
        await this.syncPowerLevels(options.windowHours, options.dryRun);
      }

      if (options.syncVotes) {
        await this.syncVotes(options.windowHours, options.dryRun);
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.stats.errors.push(`Fatal: ${msg}`);
      console.error('❌ Sync error:', error);
    }

    this.stats.endTime = new Date();
    return this.stats;
  }

  private async syncPowerLevels(windowHours: number, dryRun: boolean = false): Promise<void> {
    console.log(`\n📊 Syncing user power levels...${dryRun ? ' [DRY RUN]' : ''}`);

    const isFullSync = windowHours === 0;
    let query: string;

    if (isFullSync) {
      query = `
        SELECT fid, brnd_power_level
        FROM "${this.schema}".users
        WHERE brnd_power_level > 0
      `;
    } else {
      const windowStart = Math.floor(
        (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
      );
      query = `
        SELECT DISTINCT u.fid, u.brnd_power_level
        FROM "${this.schema}".users u
        INNER JOIN "${this.schema}".brnd_power_level_ups lup ON u.fid = lup.fid
        WHERE lup.timestamp >= ${windowStart}
      `;
    }

    const result = await this.pgClient.query(query);
    const indexerUsers = result.rows;

    console.log(`   Found ${indexerUsers.length} users to check`);
    this.stats.usersChecked = indexerUsers.length;

    if (indexerUsers.length === 0) {
      console.log('   No users to sync');
      return;
    }

    // Get FIDs
    const fids = indexerUsers.map((u: any) => u.fid);

    // Fetch MySQL users
    const [mysqlRows] = await this.mysqlConn!.execute(
      `SELECT id, fid, brndPowerLevel FROM users WHERE fid IN (${fids.join(',')})`,
    );
    const mysqlUsers = mysqlRows as any[];
    const mysqlUserMap = new Map(mysqlUsers.map((u) => [u.fid, u]));

    // Process each user
    for (const indexerUser of indexerUsers) {
      const fid = indexerUser.fid;
      const indexerLevel = indexerUser.brnd_power_level;
      const mysqlUser = mysqlUserMap.get(fid);

      if (!mysqlUser) {
        console.log(`   ⚠️ User FID ${fid} not in MySQL, skipping`);
        continue;
      }

      // Only update if indexer level is HIGHER (power levels only go up)
      if (indexerLevel > mysqlUser.brndPowerLevel) {
        console.log(
          `   ${dryRun ? '[DRY RUN] Would update' : 'Updating'} FID ${fid}: level ${mysqlUser.brndPowerLevel} -> ${indexerLevel}`,
        );

        if (!dryRun) {
          await this.mysqlConn!.execute(
            'UPDATE users SET brndPowerLevel = ? WHERE fid = ?',
            [indexerLevel, fid],
          );
        }
        this.stats.usersUpdated++;
      }
    }

    console.log(`   ✅ ${dryRun ? 'Would update' : 'Updated'} ${this.stats.usersUpdated} users`);
  }

  private async syncVotes(windowHours: number, dryRun: boolean = false): Promise<void> {
    console.log(`\n📊 Syncing votes...${dryRun ? ' [DRY RUN]' : ''}`);

    const isFullSync = windowHours === 0;
    let query: string;

    if (isFullSync) {
      query = `
        SELECT id, voter, fid, day, brand_ids, cost, block_number, transaction_hash, timestamp
        FROM "${this.schema}".votes
        ORDER BY timestamp ASC
      `;
    } else {
      const windowStart = Math.floor(
        (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
      );
      query = `
        SELECT id, voter, fid, day, brand_ids, cost, block_number, transaction_hash, timestamp
        FROM "${this.schema}".votes
        WHERE timestamp >= ${windowStart}
        ORDER BY timestamp ASC
      `;
    }

    const result = await this.pgClient.query(query);
    const indexerVotes = result.rows;

    console.log(`   Found ${indexerVotes.length} votes to check`);
    this.stats.votesChecked = indexerVotes.length;

    if (indexerVotes.length === 0) {
      console.log('   No votes to sync');
      return;
    }

    // Get existing votes from MySQL
    const txHashes = indexerVotes.map((v: any) => `'${v.transaction_hash}'`);

    // Process in chunks to avoid query too large
    const CHUNK_SIZE = 500;
    const existingTxSet = new Set<string>();

    for (let i = 0; i < txHashes.length; i += CHUNK_SIZE) {
      const chunk = txHashes.slice(i, i + CHUNK_SIZE);
      const [rows] = await this.mysqlConn!.execute(
        `SELECT transactionHash FROM user_brand_votes WHERE transactionHash IN (${chunk.join(',')})`,
      );
      (rows as any[]).forEach((r) => existingTxSet.add(r.transactionHash));
    }

    console.log(`   ${existingTxSet.size} votes already exist in MySQL`);

    // Preload user cache
    const fids = [...new Set(indexerVotes.map((v: any) => v.fid))];
    const [userRows] = await this.mysqlConn!.execute(
      `SELECT id, fid FROM users WHERE fid IN (${fids.join(',')})`,
    );
    const userFidToId = new Map((userRows as any[]).map((u) => [u.fid, u.id]));

    // Preload brand cache
    const [brandRows] = await this.mysqlConn!.execute('SELECT id FROM brands');
    const brandIds = new Set((brandRows as any[]).map((b) => b.id));

    // Process votes
    const BATCH_SIZE = 50;
    for (let i = 0; i < indexerVotes.length; i += BATCH_SIZE) {
      const batch = indexerVotes.slice(i, i + BATCH_SIZE);

      for (const vote of batch) {
        const txHash = vote.transaction_hash;

        if (existingTxSet.has(txHash)) {
          continue; // Already exists
        }

        try {
          // Parse brand IDs
          let brandIdsArray: number[];
          try {
            brandIdsArray = JSON.parse(vote.brand_ids);
          } catch {
            continue;
          }

          if (brandIdsArray.length !== 3) continue;

          // Check brands exist
          if (!brandIdsArray.every((id) => brandIds.has(id))) {
            continue;
          }

          // Get user ID
          let userId = userFidToId.get(vote.fid);
          if (!userId) {
            if (dryRun) {
              // In dry run, use a placeholder
              userId = -1;
              console.log(`   [DRY RUN] Would create user for FID ${vote.fid}`);
            } else {
              // Create minimal user
              const [insertResult] = await this.mysqlConn!.execute(
                `INSERT INTO users (fid, username, photoUrl, address, points, dailyStreak, maxDailyStreak,
                 totalPodiums, votedBrandsCount, brndPowerLevel, totalVotes, banned, powerups, verified,
                 notificationsEnabled, neynarScore, createdAt, updatedAt)
                 VALUES (?, ?, '', ?, 0, 0, 0, 0, 0, 0, 0, false, 0, false, false, 0, NOW(), NOW())`,
                [vote.fid, `user_${vote.fid}`, vote.voter],
              );
              userId = (insertResult as any).insertId;
              userFidToId.set(vote.fid, userId);
            }
          }

          // Calculate vote data
          const voteDate = new Date(Number(vote.timestamp) * 1000);
          const day = Math.floor(Number(vote.timestamp) / 86400);
          const costBigInt = BigInt(vote.cost);
          const brndPaid = Number(costBigInt / BigInt(10 ** 18));
          const rewardAmount = (costBigInt * 10n).toString();

          // Insert vote
          if (!dryRun) {
            await this.mysqlConn!.execute(
              `INSERT INTO user_brand_votes
               (transactionHash, id, userId, brand1Id, brand2Id, brand3Id, date, day,
                brndPaidWhenCreatingPodium, rewardAmount, shared, shareVerified, pointsEarned)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false, false, 3)`,
              [
                txHash,
                vote.id,
                userId,
                brandIdsArray[0],
                brandIdsArray[1],
                brandIdsArray[2],
                voteDate,
                day,
                brndPaid,
                rewardAmount,
              ],
            );
          }

          this.stats.votesInserted++;
          existingTxSet.add(txHash);
        } catch (error) {
          const msg = error instanceof Error ? error.message : String(error);
          this.stats.errors.push(`Vote ${txHash}: ${msg}`);
        }
      }

      // Progress log
      const progress = Math.min(i + BATCH_SIZE, indexerVotes.length);
      process.stdout.write(
        `\r   Processing: ${progress}/${indexerVotes.length} (${this.stats.votesInserted} inserted)`,
      );
    }

    console.log(`\n   ✅ ${dryRun ? 'Would insert' : 'Inserted'} ${this.stats.votesInserted} votes`);
  }

  private async syncBrands(windowHours: number, dryRun: boolean = false): Promise<void> {
    console.log(`\n📊 Syncing brands...${dryRun ? ' [DRY RUN]' : ''}`);

    const isFullSync = windowHours === 0;
    let query: string;

    if (isFullSync) {
      query = `
        SELECT id, fid, wallet_address, handle, metadata_hash, total_brnd_awarded,
               available_brnd, created_at, block_number, transaction_hash
        FROM "${this.schema}".brands
        ORDER BY id ASC
      `;
    } else {
      const windowStart = Math.floor(
        (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
      );
      query = `
        SELECT id, fid, wallet_address, handle, metadata_hash, total_brnd_awarded,
               available_brnd, created_at, block_number, transaction_hash
        FROM "${this.schema}".brands
        WHERE created_at >= ${windowStart}
        ORDER BY id ASC
      `;
    }

    const result = await this.pgClient.query(query);
    const indexerBrands = result.rows;

    console.log(`   Found ${indexerBrands.length} brands to check`);
    this.stats.brandsChecked = indexerBrands.length;

    if (indexerBrands.length === 0) {
      console.log('   No brands to sync');
      return;
    }

    // Get existing brands from MySQL by onChainId
    const onChainIds = indexerBrands.map((b: any) => b.id);
    const [existingRows] = await this.mysqlConn!.execute(
      `SELECT id, onChainId, metadataHash FROM brands WHERE onChainId IN (${onChainIds.join(',')})`,
    );
    const existingBrandMap = new Map(
      (existingRows as any[]).map((b) => [b.onChainId, b]),
    );

    console.log(`   ${existingBrandMap.size} brands already exist in MySQL`);

    // Get or create default category
    let [categoryRows] = await this.mysqlConn!.execute(
      `SELECT id FROM category WHERE name = 'General' LIMIT 1`,
    );
    let defaultCategoryId: number;
    if ((categoryRows as any[]).length === 0) {
      const [insertResult] = await this.mysqlConn!.execute(
        `INSERT INTO category (name) VALUES ('General')`,
      );
      defaultCategoryId = (insertResult as any).insertId;
    } else {
      defaultCategoryId = (categoryRows as any[])[0].id;
    }

    // Process each brand
    for (const indexerBrand of indexerBrands) {
      try {
        const onChainId = indexerBrand.id;
        const existingBrand = existingBrandMap.get(onChainId);

        // Normalize metadataHash (strip ipfs:// prefix if present)
        let metadataHash = indexerBrand.metadata_hash || '';
        if (metadataHash.startsWith('ipfs://')) {
          metadataHash = metadataHash.slice(7);
        } else if (metadataHash.startsWith('/ipfs/')) {
          metadataHash = metadataHash.slice(6);
        }

        if (existingBrand) {
          // Brand exists - check if metadata hash changed
          if (existingBrand.metadataHash !== metadataHash && metadataHash) {
            console.log(`   ${dryRun ? '[DRY RUN] Would update' : 'Updating'} brand onChainId ${onChainId}: metadata changed`);

            // Fetch metadata from IPFS
            const metadata = await this.fetchIpfsMetadata(metadataHash);

            // Update the brand
            if (!dryRun) {
              await this.mysqlConn!.execute(
                `UPDATE brands SET
                  metadataHash = ?,
                  onChainHandle = ?,
                  onChainFid = ?,
                  onChainWalletAddress = ?,
                  totalBrndAwarded = ?,
                  availableBrnd = ?,
                  name = COALESCE(?, name),
                  description = COALESCE(?, description),
                  imageUrl = COALESCE(?, imageUrl),
                  url = COALESCE(?, url),
                  profile = COALESCE(?, profile),
                  channel = COALESCE(?, channel),
                  updatedAt = NOW()
                WHERE onChainId = ?`,
                [
                  metadataHash,
                  indexerBrand.handle,
                  indexerBrand.fid,
                  indexerBrand.wallet_address,
                  indexerBrand.total_brnd_awarded?.toString() || '0',
                  indexerBrand.available_brnd?.toString() || '0',
                  metadata.name || null,
                  metadata.description || null,
                  metadata.imageUrl || null,
                  metadata.url || null,
                  metadata.profile || null,
                  metadata.channel || null,
                  onChainId,
                ],
              );
            }
            this.stats.brandsUpdated++;
          }
          continue;
        }

        // Brand doesn't exist - create it
        console.log(`   ${dryRun ? '[DRY RUN] Would create' : 'Creating'} brand onChainId ${onChainId}: ${indexerBrand.handle}`);

        // Fetch metadata from IPFS
        const metadata = await this.fetchIpfsMetadata(metadataHash);

        // Determine profile/channel
        let profile = metadata.profile || '';
        let channel = metadata.channel || '';
        let queryType = metadata.queryType ?? 0;

        if (!profile && !channel) {
          channel = `/${indexerBrand.handle}`;
          queryType = 0;
        }

        // Insert the brand
        if (!dryRun) {
          await this.mysqlConn!.execute(
            `INSERT INTO brands (
              onChainId, onChainHandle, onChainFid, onChainWalletAddress, onChainCreatedAt,
              metadataHash, name, url, warpcastUrl, description, imageUrl, profile, channel,
              queryType, followerCount, categoryId, score, stateScore, scoreDay, stateScoreDay,
              scoreWeek, stateScoreWeek, scoreMonth, stateScoreMonth, ranking, rankingWeek,
              rankingMonth, bonusPoints, banned, currentRanking, totalBrndAwarded, availableBrnd,
              createdAt, updatedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
            [
              indexerBrand.id,
              indexerBrand.handle,
              indexerBrand.fid,
              indexerBrand.wallet_address,
              new Date(Number(indexerBrand.created_at) * 1000),
              metadataHash,
              metadata.name || indexerBrand.handle,
              metadata.url || '',
              metadata.warpcastUrl || metadata.url || '',
              metadata.description || '',
              metadata.imageUrl || '',
              profile,
              channel,
              queryType,
              metadata.followerCount || 0,
              defaultCategoryId,
              0, // score
              0, // stateScore
              0, // scoreDay
              0, // stateScoreDay
              0, // scoreWeek
              0, // stateScoreWeek
              0, // scoreMonth
              0, // stateScoreMonth
              '0', // ranking
              0, // rankingWeek
              0, // rankingMonth
              0, // bonusPoints
              0, // banned
              0, // currentRanking
              indexerBrand.total_brnd_awarded?.toString() || '0',
              indexerBrand.available_brnd?.toString() || '0',
            ],
          );
          console.log(`   ✅ Created brand: ${metadata.name || indexerBrand.handle} (onChainId: ${onChainId})`);
        }

        this.stats.brandsCreated++;
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        this.stats.errors.push(`Brand ${indexerBrand.id} (${indexerBrand.handle}): ${msg}`);
        console.error(`   ❌ Error syncing brand ${indexerBrand.id}: ${msg}`);
      }
    }

    console.log(`   ✅ Brands sync complete: ${this.stats.brandsCreated} ${dryRun ? 'would be' : ''} created, ${this.stats.brandsUpdated} ${dryRun ? 'would be' : ''} updated`);
  }

  private async fetchIpfsMetadata(metadataHash: string): Promise<any> {
    if (!metadataHash) return {};

    const gateways = [
      `https://ipfs.io/ipfs/${metadataHash}`,
      `https://cloudflare-ipfs.com/ipfs/${metadataHash}`,
      `https://gateway.pinata.cloud/ipfs/${metadataHash}`,
    ];

    for (const gateway of gateways) {
      try {
        const response = await fetch(gateway);
        if (response.ok) {
          return await response.json();
        }
      } catch {
        // Try next gateway
      }
    }

    console.log(`   ⚠️ Failed to fetch IPFS metadata: ${metadataHash}`);
    return {};
  }

  printSummary(): void {
    const duration = this.stats.endTime
      ? Math.round(
          (this.stats.endTime.getTime() - this.stats.startTime.getTime()) /
            1000,
        )
      : 0;

    console.log('\n' + '='.repeat(50));
    console.log('         SYNC SUMMARY');
    console.log('='.repeat(50));
    console.log(`Duration:           ${duration}s`);
    console.log(`Brands checked:     ${this.stats.brandsChecked}`);
    console.log(`Brands created:     ${this.stats.brandsCreated}`);
    console.log(`Brands updated:     ${this.stats.brandsUpdated}`);
    console.log(`Users checked:      ${this.stats.usersChecked}`);
    console.log(`Users updated:      ${this.stats.usersUpdated}`);
    console.log(`Votes checked:      ${this.stats.votesChecked}`);
    console.log(`Votes inserted:     ${this.stats.votesInserted}`);
    console.log(`Errors:             ${this.stats.errors.length}`);
    console.log('='.repeat(50));

    if (this.stats.errors.length > 0) {
      console.log('\n⚠️ Errors:');
      this.stats.errors.slice(0, 10).forEach((err, i) => {
        console.log(`   ${i + 1}. ${err}`);
      });
      if (this.stats.errors.length > 10) {
        console.log(`   ... and ${this.stats.errors.length - 10} more`);
      }
    } else {
      console.log('\n✅ No errors!');
    }
  }
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  const syncer = new IndexerSyncer();

  try {
    // Check for CLI args (for non-interactive mode)
    const args = process.argv.slice(2);

    let options: SyncOptions;

    const dryRun = args.includes('--dry-run');

    if (args.includes('--48h')) {
      options = { windowHours: 48, syncBrands: true, syncPowerLevels: true, syncVotes: true, dryRun };
      console.log(`\n🔄 Running 48-hour sync (non-interactive mode)${dryRun ? ' [DRY RUN]' : ''}`);
    } else if (args.includes('--full')) {
      options = { windowHours: 0, syncBrands: true, syncPowerLevels: true, syncVotes: true, dryRun };
      console.log(`\n🔄 Running FULL sync (non-interactive mode)${dryRun ? ' [DRY RUN]' : ''}`);
    } else if (args.includes('--7d')) {
      options = { windowHours: 168, syncBrands: true, syncPowerLevels: true, syncVotes: true, dryRun };
      console.log(`\n🔄 Running 7-day sync (non-interactive mode)${dryRun ? ' [DRY RUN]' : ''}`);
    } else {
      // Interactive mode
      options = await promptForOptions();
    }

    await syncer.connect();
    await syncer.sync(options);
    syncer.printSummary();
  } catch (error) {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await syncer.disconnect();
    rl.close();
  }
}

main();
