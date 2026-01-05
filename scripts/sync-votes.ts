#!/usr/bin/env bun

/**
 * Vote Synchronization Script
 *
 * Syncs votes from the PostgreSQL indexer database (source of truth)
 * to the MySQL production database (legacy backend).
 *
 * The indexer database contains actual on-chain vote data, while the
 * production database was built before on-chain functionality existed.
 *
 * Schema mapping:
 * - Indexer: votes table (PostgreSQL, on-chain data)
 * - Production: user_brand_votes table (MySQL, legacy structure)
 */

import { Client } from 'pg';
import * as mysql from 'mysql2/promise';
import { createHash } from 'crypto';

// Configuration interfaces
interface IndexerConfig {
  connectionString: string;
  schema: string;
}

interface MySQLConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  ssl?: boolean;
}

interface SyncStats {
  totalIndexerVotes: number;
  totalProductionVotes: number;
  missingVotes: number;
  insertedVotes: number;
  updatedVotes: number;
  errors: number;
}

// Main sync class
class VoteSyncer {
  private indexerClient: Client;
  private mysqlConnection: mysql.Connection;
  private userCache = new Map<number, number>(); // fid -> userId
  private brandCache = new Set<number>(); // brandId
  private stats: SyncStats = {
    totalIndexerVotes: 0,
    totalProductionVotes: 0,
    missingVotes: 0,
    insertedVotes: 0,
    updatedVotes: 0,
    errors: 0,
  };

  constructor(
    private indexerConfig: IndexerConfig,
    private mysqlConfig: MySQLConfig,
  ) {
    this.indexerClient = new Client({
      connectionString: indexerConfig.connectionString,
    });
  }

  /**
   * Initialize database connections
   */
  async connect(): Promise<void> {
    console.log('🔌 Connecting to databases...');

    // Connect to PostgreSQL indexer
    await this.indexerClient.connect();
    console.log('✅ Connected to PostgreSQL indexer database');

    // Get production database configuration from environment variables
    const prodConfig = {
      host: process.env.PROD_DATABASE_HOST,
      port: parseInt(process.env.PROD_DATABASE_PORT || '3306', 10),
      database: process.env.PROD_DATABASE_NAME,
      user: process.env.PROD_DATABASE_USER,
      password: process.env.PROD_DATABASE_PASSWORD,
    };

    // Validate that all required environment variables are set
    if (
      !prodConfig.host ||
      !prodConfig.database ||
      !prodConfig.user ||
      !prodConfig.password
    ) {
      console.error(
        '❌ ERROR: Missing required production database environment variables:',
      );
      console.error(
        '   Required: PROD_DATABASE_HOST, PROD_DATABASE_NAME, PROD_DATABASE_USER, PROD_DATABASE_PASSWORD',
      );
      console.error('   Optional: PROD_DATABASE_PORT (defaults to 3306)');
      process.exit(1);
    }

    // Validate that production and new database are different
    const newConfig = {
      host: process.env.DATABASE_HOST,
      database: process.env.DATABASE_NAME,
    };

    if (
      prodConfig.host === newConfig.host &&
      prodConfig.database === newConfig.database
    ) {
      console.error(
        '❌ ERROR: Production and new database configurations are the same!',
      );
      console.error(
        '   This is a safety check to prevent accidental data loss.',
      );
      console.error(
        '   Please ensure PROD_DATABASE_* and DATABASE_* point to different databases.',
      );
      process.exit(1);
    }

    // Connect to MySQL production database
    this.mysqlConnection = await mysql.createConnection({
      host: prodConfig.host,
      port: prodConfig.port,
      database: prodConfig.database,
      user: prodConfig.user,
      password: prodConfig.password,
    });
    console.log('✅ Connected to MySQL production database');

    // Preload caches for performance
    await this.preloadCaches();
  }

  /**
   * Preload user and brand caches for performance
   */
  async preloadCaches(): Promise<void> {
    console.log('🔄 Preloading caches...');

    // Load all users into cache
    const [users] = await this.mysqlConnection.execute(
      'SELECT id, fid FROM users WHERE fid IS NOT NULL',
    );
    (users as any[]).forEach((user) => {
      if (user.fid) {
        this.userCache.set(user.fid, user.id);
      }
    });
    console.log(`✅ Loaded ${this.userCache.size} users into cache`);

    // Load all brands into cache
    const [brands] = await this.mysqlConnection.execute(
      'SELECT id FROM brands',
    );
    (brands as any[]).forEach((brand) => {
      this.brandCache.add(brand.id);
    });
    console.log(`✅ Loaded ${this.brandCache.size} brands into cache`);
  }

  /**
   * Close database connections
   */
  async disconnect(): Promise<void> {
    console.log('🔌 Closing database connections...');
    await this.indexerClient.end();
    await this.mysqlConnection.end();
    console.log('✅ Database connections closed');
  }

  /**
   * Get all votes from the indexer database
   */
  async getIndexerVotes(): Promise<any[]> {
    const query = `
      SELECT 
        id,
        voter,
        fid,
        day,
        brand_ids,
        cost,
        block_number,
        transaction_hash,
        timestamp
      FROM "${this.indexerConfig.schema}".votes 
      ORDER BY timestamp ASC
    `;

    const result = await this.indexerClient.query(query);
    return result.rows;
  }

  /**
   * Get existing votes from production database
   */
  async getProductionVotes(): Promise<Map<string, any>> {
    const query = `
      SELECT transactionHash, id, userId, brand1Id, brand2Id, brand3Id, date, shared, castHash
      FROM user_brand_votes
    `;

    const [rows] = await this.mysqlConnection.execute(query);
    const voteMap = new Map();

    (rows as any[]).forEach((vote) => {
      voteMap.set(vote.transactionHash, vote);
    });

    return voteMap;
  }

  /**
   * Get or create user by FID (cached)
   */
  async getOrCreateUser(
    fid: number,
    voterAddress: string,
  ): Promise<number | null> {
    // Check cache first
    if (this.userCache.has(fid)) {
      return this.userCache.get(fid)!;
    }

    // If user doesn't exist, try to create one
    // Note: We'll create a minimal user record - the indexer service will fill in details later
    try {
      const [result] = await this.mysqlConnection.execute(
        `INSERT INTO users (
          fid, username, photoUrl, address, points, role, 
          dailyStreak, maxDailyStreak, totalPodiums, votedBrandsCount,
          brndPowerLevel, totalVotes, banned, powerups, verified, 
          notificationsEnabled, neynarScore, createdAt, updatedAt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
        [
          fid,
          `user_${fid}`, // placeholder username
          '', // placeholder photoUrl
          voterAddress,
          0, // points
          'user', // role
          0, // dailyStreak
          0, // maxDailyStreak
          0, // totalPodiums
          0, // votedBrandsCount
          1, // brndPowerLevel (default)
          0, // totalVotes
          false, // banned
          0, // powerups
          false, // verified
          false, // notificationsEnabled
          0.0, // neynarScore
        ],
      );

      const userId = (result as any).insertId;
      this.userCache.set(fid, userId);
      return userId;
    } catch (error) {
      console.error(`❌ Error creating user for FID ${fid}:`, error);
      return null;
    }
  }

  /**
   * Check if brand exists (cached)
   */
  brandExists(brandId: number): boolean {
    return this.brandCache.has(brandId);
  }

  /**
   * Parse brand IDs from JSON string
   */
  parseBrandIds(brandIdsJson: string): number[] {
    try {
      const parsed = JSON.parse(brandIdsJson);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  /**
   * Convert Unix timestamp to MySQL datetime
   */
  timestampToMysqlDate(timestamp: string | number): string {
    const date = new Date(Number(timestamp) * 1000);
    return date.toISOString().slice(0, 19).replace('T', ' ');
  }

  /**
   * Calculate day number from timestamp (matches blockchain logic)
   */
  calculateDay(timestamp: string | number): number {
    return Math.floor(Number(timestamp) / 86400);
  }

  /**
   * Calculate reward amount in wei (cost * 10)
   */
  calculateRewardAmount(cost: string): string {
    const costBigInt = BigInt(cost);
    const rewardBigInt = costBigInt * 10n;
    return rewardBigInt.toString();
  }

  /**
   * Convert cost from wei to BRND units
   */
  weiToBrnd(weiAmount: string): number {
    const wei = BigInt(weiAmount);
    const brnd = wei / BigInt(10 ** 18);
    return Number(brnd);
  }

  /**
   * Insert a vote into the production database
   */
  async insertVote(indexerVote: any, userId: number): Promise<boolean> {
    try {
      const brandIds = this.parseBrandIds(indexerVote.brand_ids);
      const voteDate = this.timestampToMysqlDate(indexerVote.timestamp);
      const day = this.calculateDay(indexerVote.timestamp);
      const brndPaid = this.weiToBrnd(indexerVote.cost);
      const rewardAmount = this.calculateRewardAmount(indexerVote.cost);

      // Validate that we have exactly 3 brand IDs
      if (brandIds.length !== 3) {
        console.warn(
          `⚠️ Skipping vote ${indexerVote.id}: expected 3 brands, got ${brandIds.length}`,
        );
        return false;
      }

      // Validate that all brands exist
      const missingBrands = brandIds.filter(
        (brandId) => !this.brandExists(brandId),
      );
      if (missingBrands.length > 0) {
        console.warn(
          `⚠️ Skipping vote ${indexerVote.id}: missing brands ${missingBrands.join(', ')}`,
        );
        return false;
      }

      // Insert the vote
      await this.mysqlConnection.execute(
        `INSERT INTO user_brand_votes (
          transactionHash, id, userId, brand1Id, brand2Id, brand3Id, 
          date, shared, brndPaidWhenCreatingPodium, rewardAmount, day,
          shareVerified, castHash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          indexerVote.transaction_hash,
          indexerVote.id,
          userId,
          brandIds[0],
          brandIds[1],
          brandIds[2],
          voteDate,
          false, // shared (will be updated when reward is claimed)
          brndPaid,
          rewardAmount,
          day,
          false, // shareVerified (will be updated when reward is claimed)
          null, // castHash (will be updated when reward is claimed)
        ],
      );

      this.stats.insertedVotes++;
      return true;
    } catch (error) {
      console.error(`❌ Error inserting vote ${indexerVote.id}:`, error);
      this.stats.errors++;
      return false;
    }
  }

  /**
   * Update vote data (for cases where vote exists but might need updates)
   */
  async updateVote(
    indexerVote: any,
    existingVote: any,
    userId: number,
  ): Promise<boolean> {
    try {
      const brandIds = this.parseBrandIds(indexerVote.brand_ids);
      const voteDate = this.timestampToMysqlDate(indexerVote.timestamp);
      const day = this.calculateDay(indexerVote.timestamp);
      const brndPaid = this.weiToBrnd(indexerVote.cost);
      const rewardAmount = this.calculateRewardAmount(indexerVote.cost);

      // Only update if there are actual differences
      const needsUpdate =
        existingVote.userId !== userId ||
        existingVote.brand1Id !== brandIds[0] ||
        existingVote.brand2Id !== brandIds[1] ||
        existingVote.brand3Id !== brandIds[2] ||
        existingVote.brndPaidWhenCreatingPodium !== brndPaid ||
        existingVote.day !== day;

      if (!needsUpdate) {
        return true; // No update needed
      }

      await this.mysqlConnection.execute(
        `UPDATE user_brand_votes 
         SET userId = ?, brand1Id = ?, brand2Id = ?, brand3Id = ?, 
             date = ?, brndPaidWhenCreatingPodium = ?, rewardAmount = ?, 
             day = ?
         WHERE transactionHash = ?`,
        [
          userId,
          brandIds[0],
          brandIds[1],
          brandIds[2],
          voteDate,
          brndPaid,
          rewardAmount,
          day,
          indexerVote.transaction_hash,
        ],
      );

      this.stats.updatedVotes++;
      return true;
    } catch (error) {
      console.error(`❌ Error updating vote ${indexerVote.id}:`, error);
      this.stats.errors++;
      return false;
    }
  }

  /**
   * Main synchronization logic with batch processing
   */
  async sync(): Promise<void> {
    console.log('🚀 Starting vote synchronization...\n');

    // Get data from both databases
    console.log('📊 Fetching data from databases...');
    const indexerVotes = await this.getIndexerVotes();
    const productionVotes = await this.getProductionVotes();

    this.stats.totalIndexerVotes = indexerVotes.length;
    this.stats.totalProductionVotes = productionVotes.size;

    console.log(
      `📈 Found ${this.stats.totalIndexerVotes} votes in indexer database`,
    );
    console.log(
      `📊 Found ${this.stats.totalProductionVotes} votes in production database\n`,
    );

    // Process votes in batches for better performance
    const BATCH_SIZE = 50;
    for (let i = 0; i < indexerVotes.length; i += BATCH_SIZE) {
      const batch = indexerVotes.slice(
        i,
        Math.min(i + BATCH_SIZE, indexerVotes.length),
      );

      console.log(
        `⏳ Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(indexerVotes.length / BATCH_SIZE)} (${Math.round(((i + batch.length) / indexerVotes.length) * 100)}%)`,
      );

      await this.processBatch(batch, productionVotes);
    }

    console.log('\n✅ Vote synchronization completed!\n');
    this.printStats();
  }

  /**
   * Process a batch of votes
   */
  async processBatch(
    batch: any[],
    productionVotes: Map<string, any>,
  ): Promise<void> {
    const promises = batch.map(async (indexerVote) => {
      try {
        const existingVote = productionVotes.get(indexerVote.transaction_hash);

        // Get or create user
        const userId = await this.getOrCreateUser(
          indexerVote.fid,
          indexerVote.voter,
        );
        if (!userId) {
          console.warn(
            `⚠️ Could not get/create user for FID ${indexerVote.fid}, skipping vote`,
          );
          this.stats.errors++;
          return;
        }

        if (!existingVote) {
          // Vote doesn't exist in production, insert it
          this.stats.missingVotes++;
          await this.insertVote(indexerVote, userId);
        } else {
          // Vote exists, check if it needs updating
          await this.updateVote(indexerVote, existingVote, userId);
        }
      } catch (error) {
        console.error(
          `❌ Error processing vote ${indexerVote.transaction_hash}:`,
          error,
        );
        this.stats.errors++;
      }
    });

    await Promise.all(promises);
  }

  /**
   * Print synchronization statistics
   */
  private printStats(): void {
    console.log('📊 SYNCHRONIZATION STATISTICS:');
    console.log('=====================================');
    console.log(`Total votes in indexer:     ${this.stats.totalIndexerVotes}`);
    console.log(
      `Total votes in production:  ${this.stats.totalProductionVotes}`,
    );
    console.log(`Missing votes found:        ${this.stats.missingVotes}`);
    console.log(`Votes inserted:            ${this.stats.insertedVotes}`);
    console.log(`Votes updated:             ${this.stats.updatedVotes}`);
    console.log(`Errors:                    ${this.stats.errors}`);
    console.log('=====================================');

    if (this.stats.errors > 0) {
      console.log(
        `⚠️  ${this.stats.errors} errors occurred during sync. Check logs above for details.`,
      );
    } else {
      console.log('🎉 No errors occurred during synchronization!');
    }
  }

  /**
   * Dry run - shows what would be synced without making changes
   */
  async dryRun(): Promise<void> {
    console.log('🔍 RUNNING DRY RUN (no changes will be made)...\n');

    const indexerVotes = await this.getIndexerVotes();
    const productionVotes = await this.getProductionVotes();

    this.stats.totalIndexerVotes = indexerVotes.length;
    this.stats.totalProductionVotes = productionVotes.size;

    console.log(
      `📈 Indexer database has ${this.stats.totalIndexerVotes} votes`,
    );
    console.log(
      `📊 Production database has ${this.stats.totalProductionVotes} votes`,
    );

    // Find missing votes
    const missingVotes = [];
    const existingVotes = [];

    for (const indexerVote of indexerVotes) {
      if (productionVotes.has(indexerVote.transaction_hash)) {
        existingVotes.push(indexerVote);
      } else {
        missingVotes.push(indexerVote);
      }
    }

    console.log(`\n📊 DRY RUN RESULTS:`);
    console.log(`Votes missing in production: ${missingVotes.length}`);
    console.log(`Votes already in production: ${existingVotes.length}`);

    if (missingVotes.length > 0) {
      console.log(`\n📝 First 5 missing votes:`);
      missingVotes.slice(0, 5).forEach((vote, index) => {
        console.log(
          `  ${index + 1}. TX: ${vote.transaction_hash.slice(0, 10)}... FID: ${vote.fid} Day: ${vote.day}`,
        );
      });
    }

    console.log(
      '\n✅ Dry run completed! Use --sync to perform actual synchronization.',
    );
  }
}

// CLI handling
async function main() {
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run');
  const isSync = args.includes('--sync');

  if (!isDryRun && !isSync) {
    console.log('🤖 Vote Synchronization Script');
    console.log('=============================');
    console.log('Usage:');
    console.log('  --dry-run    Show what would be synced (no changes)');
    console.log('  --sync       Perform actual synchronization');
    console.log('');
    console.log('Example:');
    console.log('  npm run sync-votes -- --dry-run');
    console.log('  npm run sync-votes -- --sync');
    process.exit(1);
  }

  // Configuration from environment
  const indexerConfig: IndexerConfig = {
    connectionString: process.env.INDEXER_DB_URL!,
    schema: process.env.INDEXER_DB_SCHEMA || 'public',
  };

  const mysqlConfig: MySQLConfig = {
    host: process.env.DATABASE_HOST!,
    port: parseInt(process.env.DATABASE_PORT || '3306'),
    user: process.env.DATABASE_USER!,
    password: process.env.DATABASE_PASSWORD!,
    database: process.env.DATABASE_NAME!,
    ssl: process.env.DATABASE_SSL === 'true',
  };

  // Validate configuration
  if (!indexerConfig.connectionString) {
    console.error('❌ INDEXER_DB_URL environment variable is required');
    process.exit(1);
  }

  if (
    !mysqlConfig.host ||
    !mysqlConfig.user ||
    !mysqlConfig.password ||
    !mysqlConfig.database
  ) {
    console.error('❌ MySQL database environment variables are required');
    process.exit(1);
  }

  const syncer = new VoteSyncer(indexerConfig, mysqlConfig);

  try {
    await syncer.connect();

    if (isDryRun) {
      await syncer.dryRun();
    } else {
      await syncer.sync();
    }
  } catch (error) {
    console.error('❌ Synchronization failed:', error);
    process.exit(1);
  } finally {
    await syncer.disconnect();
  }
}

// Run the script
if (require.main === module) {
  main();
}
