#!/usr/bin/env bun

/**
 * Indexer Sync Script
 *
 * Syncs data from the PostgreSQL indexer database (source of truth)
 * to the MySQL production database.
 *
 * Syncs:
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
  syncPowerLevels: boolean;
  syncVotes: boolean;
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
  console.log('  1. Both power levels and votes (recommended)');
  console.log('  2. Only power levels');
  console.log('  3. Only votes');
  console.log('');

  const syncChoice = await prompt('Enter choice (1-3): ');

  let syncPowerLevels = true;
  let syncVotes = true;

  switch (syncChoice.trim()) {
    case '2':
      syncVotes = false;
      break;
    case '3':
      syncPowerLevels = false;
      break;
    default:
      // Default to both
      break;
  }

  // Confirm
  console.log('\n' + '='.repeat(50));
  console.log('SYNC CONFIGURATION:');
  console.log(`  Window: ${windowHours === 0 ? 'FULL SYNC' : `Last ${windowHours} hours`}`);
  console.log(`  Sync power levels: ${syncPowerLevels ? 'Yes' : 'No'}`);
  console.log(`  Sync votes: ${syncVotes ? 'Yes' : 'No'}`);
  console.log('='.repeat(50));

  const confirm = await prompt('\nProceed with sync? (y/n): ');
  if (confirm.toLowerCase() !== 'y') {
    console.log('Sync cancelled.');
    process.exit(0);
  }

  return { windowHours, syncPowerLevels, syncVotes };
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

    try {
      if (options.syncPowerLevels) {
        await this.syncPowerLevels(options.windowHours);
      }

      if (options.syncVotes) {
        await this.syncVotes(options.windowHours);
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.stats.errors.push(`Fatal: ${msg}`);
      console.error('❌ Sync error:', error);
    }

    this.stats.endTime = new Date();
    return this.stats;
  }

  private async syncPowerLevels(windowHours: number): Promise<void> {
    console.log('\n📊 Syncing user power levels...');

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

      if (mysqlUser.brndPowerLevel !== indexerLevel) {
        console.log(
          `   Updating FID ${fid}: level ${mysqlUser.brndPowerLevel} -> ${indexerLevel}`,
        );

        await this.mysqlConn!.execute(
          'UPDATE users SET brndPowerLevel = ? WHERE fid = ?',
          [indexerLevel, fid],
        );
        this.stats.usersUpdated++;
      }
    }

    console.log(`   ✅ Updated ${this.stats.usersUpdated} users`);
  }

  private async syncVotes(windowHours: number): Promise<void> {
    console.log('\n📊 Syncing votes...');

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

          // Calculate vote data
          const voteDate = new Date(Number(vote.timestamp) * 1000);
          const day = Math.floor(Number(vote.timestamp) / 86400);
          const costBigInt = BigInt(vote.cost);
          const brndPaid = Number(costBigInt / BigInt(10 ** 18));
          const rewardAmount = (costBigInt * 10n).toString();

          // Insert vote
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

    console.log(`\n   ✅ Inserted ${this.stats.votesInserted} votes`);
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

    if (args.includes('--48h')) {
      options = { windowHours: 48, syncPowerLevels: true, syncVotes: true };
      console.log('\n🔄 Running 48-hour sync (non-interactive mode)');
    } else if (args.includes('--full')) {
      options = { windowHours: 0, syncPowerLevels: true, syncVotes: true };
      console.log('\n🔄 Running FULL sync (non-interactive mode)');
    } else if (args.includes('--7d')) {
      options = { windowHours: 168, syncPowerLevels: true, syncVotes: true };
      console.log('\n🔄 Running 7-day sync (non-interactive mode)');
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
