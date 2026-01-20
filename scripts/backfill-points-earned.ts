#!/usr/bin/env bun

/**
 * Backfill Points Earned Script
 *
 * Calculates and backfills the `pointsEarned` column for all historical votes.
 *
 * SEASON 1 (votes before Dec 13, 2025 06:50:00 UTC):
 *   - shared = true  → 6 points
 *   - shared = false → 3 points
 *
 * SEASON 2 (votes on or after Dec 13, 2025 06:50:00 UTC):
 *   - Unclaimed: 3 points (voting only)
 *   - Claimed: 3 + (brndPowerLevel at claim time × 3)
 *
 * For Season 2, we look up the user's brndPowerLevel at the time of claiming
 * by querying the indexer's brnd_power_level_ups table.
 *
 * Usage:
 *   bun run scripts/backfill-points-earned.ts
 *   bun run scripts/backfill-points-earned.ts --dry-run
 *
 * Environment variables required:
 *   - INDEXER_DB_URL: PostgreSQL connection string
 *   - INDEXER_DB_SCHEMA: Schema name (default: public)
 *   - DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME: MySQL
 */

import { Client } from 'pg';
import * as mysql from 'mysql2/promise';

// ============================================================================
// Constants
// ============================================================================

// Season 2 started at Dec 13, 2025 06:50:00 UTC
const SEASON_2_START_TIMESTAMP = 1765608600;
const SEASON_2_START_DATE = new Date(SEASON_2_START_TIMESTAMP * 1000);

// ============================================================================
// Types
// ============================================================================

interface BackfillStats {
  totalVotes: number;
  season1Votes: number;
  season2Votes: number;
  season1Updated: number;
  season2Updated: number;
  season2Unclaimed: number;
  season2Claimed: number;
  alreadyHasPoints: number;
  errors: string[];
  startTime: Date;
  endTime?: Date;
}

interface Vote {
  transactionHash: string;
  date: Date;
  shared: boolean;
  claimedAt: Date | null;
  pointsEarned: number | null;
  userFid: number;
}

interface LevelUp {
  fid: number;
  newLevel: number;
  timestamp: number;
}

// ============================================================================
// Main Class
// ============================================================================

class PointsBackfiller {
  protected pgClient: Client;
  protected mysqlConn: mysql.Connection | null = null;
  protected schema: string;
  protected stats: BackfillStats;
  protected isDryRun: boolean;

  // Cache for user level-ups from indexer
  protected levelUpCache: Map<number, LevelUp[]> = new Map();

  constructor(isDryRun: boolean = false) {
    const connectionString = process.env.INDEXER_DB_URL;
    if (!connectionString) {
      throw new Error('INDEXER_DB_URL environment variable is required');
    }

    this.pgClient = new Client({ connectionString });
    this.schema = process.env.INDEXER_DB_SCHEMA || 'public';
    this.isDryRun = isDryRun;
    this.stats = {
      totalVotes: 0,
      season1Votes: 0,
      season2Votes: 0,
      season1Updated: 0,
      season2Updated: 0,
      season2Unclaimed: 0,
      season2Claimed: 0,
      alreadyHasPoints: 0,
      errors: [],
      startTime: new Date(),
    };
  }

  async connect(): Promise<void> {
    console.log('\n🔌 Connecting to databases...');

    // Connect to PostgreSQL indexer
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

  /**
   * Preload all level-ups from the indexer for efficient lookups
   */
  async preloadLevelUps(): Promise<void> {
    console.log('\n📊 Preloading level-ups from indexer...');

    const query = `
      SELECT fid, new_level, timestamp
      FROM "${this.schema}".brnd_power_level_ups
      ORDER BY fid, timestamp ASC
    `;

    const result = await this.pgClient.query(query);
    const levelUps = result.rows;

    console.log(`   Found ${levelUps.length} level-up events`);

    // Group by FID
    for (const row of levelUps) {
      const fid = row.fid;
      if (!this.levelUpCache.has(fid)) {
        this.levelUpCache.set(fid, []);
      }
      this.levelUpCache.get(fid)!.push({
        fid: row.fid,
        newLevel: row.new_level,
        timestamp: Number(row.timestamp),
      });
    }

    console.log(`   Cached level-ups for ${this.levelUpCache.size} users`);
  }

  /**
   * Get user's brndPowerLevel at a specific timestamp
   * Returns 0 if no level-ups found before that time
   */
  getBrndPowerLevelAtTime(fid: number, timestamp: Date): number {
    const levelUps = this.levelUpCache.get(fid);

    if (!levelUps || levelUps.length === 0) {
      return 0; // No level-ups recorded, user was level 0
    }

    const targetTimestamp = Math.floor(timestamp.getTime() / 1000);

    // Find the most recent level-up that happened BEFORE or AT the target timestamp
    let level = 0;
    for (const levelUp of levelUps) {
      if (levelUp.timestamp <= targetTimestamp) {
        level = levelUp.newLevel;
      } else {
        break; // Level-ups are sorted by timestamp, so we can stop here
      }
    }

    return level;
  }

  /**
   * Calculate points earned for a vote
   */
  calculatePointsEarned(vote: Vote): number {
    const voteTimestamp = Math.floor(vote.date.getTime() / 1000);

    // Season 1: before Dec 13, 2025 06:50:00 UTC
    if (voteTimestamp < SEASON_2_START_TIMESTAMP) {
      return vote.shared ? 6 : 3;
    }

    // Season 2: on or after Dec 13, 2025 06:50:00 UTC
    if (!vote.claimedAt) {
      // Unclaimed: just voting points
      return 3;
    }

    // Claimed: 3 + (brndPowerLevel at claim time × 3)
    const levelAtClaim = this.getBrndPowerLevelAtTime(vote.userFid, vote.claimedAt);
    return 3 + (levelAtClaim * 3);
  }

  /**
   * Get all votes that need backfilling
   */
  async getVotesToBackfill(): Promise<Vote[]> {
    console.log('\n📊 Fetching votes to backfill...');

    // Get votes that don't have pointsEarned set (NULL)
    // Also get votes where we might want to recalculate (optional)
    const [rows] = await this.mysqlConn!.execute(`
      SELECT
        v.transactionHash,
        v.date,
        v.shared,
        v.claimedAt,
        v.pointsEarned,
        u.fid as userFid
      FROM user_brand_votes v
      INNER JOIN users u ON v.userId = u.id
      WHERE v.pointsEarned IS NULL
      ORDER BY v.date ASC
    `);

    const votes = (rows as any[]).map((row) => ({
      transactionHash: row.transactionHash,
      date: new Date(row.date),
      shared: Boolean(row.shared),
      claimedAt: row.claimedAt ? new Date(row.claimedAt) : null,
      pointsEarned: row.pointsEarned,
      userFid: row.userFid,
    }));

    console.log(`   Found ${votes.length} votes with NULL pointsEarned`);

    return votes;
  }

  /**
   * Update a vote's pointsEarned in the database
   */
  async updateVotePoints(
    transactionHash: string,
    pointsEarned: number,
  ): Promise<void> {
    if (this.isDryRun) {
      return;
    }

    await this.mysqlConn!.execute(
      'UPDATE user_brand_votes SET pointsEarned = ? WHERE transactionHash = ?',
      [pointsEarned, transactionHash],
    );
  }

  /**
   * Bulk update Season 1 votes (much faster than one-by-one)
   */
  async bulkUpdateSeason1(): Promise<void> {
    console.log('\n📝 Processing Season 1 votes (bulk update)...');

    const season2StartDate = SEASON_2_START_DATE.toISOString().slice(0, 19).replace('T', ' ');

    if (this.isDryRun) {
      // Just count what would be updated
      const [sharedRows] = await this.mysqlConn!.execute(`
        SELECT COUNT(*) as cnt FROM user_brand_votes
        WHERE date < ? AND shared = 1 AND pointsEarned IS NULL
      `, [season2StartDate]);
      const [notSharedRows] = await this.mysqlConn!.execute(`
        SELECT COUNT(*) as cnt FROM user_brand_votes
        WHERE date < ? AND shared = 0 AND pointsEarned IS NULL
      `, [season2StartDate]);

      const sharedCount = (sharedRows as any[])[0].cnt;
      const notSharedCount = (notSharedRows as any[])[0].cnt;

      this.stats.season1Votes = sharedCount + notSharedCount;
      this.stats.season1Updated = sharedCount + notSharedCount;
      console.log(`   Would update ${sharedCount} shared votes → 6 points`);
      console.log(`   Would update ${notSharedCount} non-shared votes → 3 points`);
      return;
    }

    // Bulk update: shared = true → 6 points
    const [sharedResult] = await this.mysqlConn!.execute(`
      UPDATE user_brand_votes
      SET pointsEarned = 6
      WHERE date < ? AND shared = 1 AND pointsEarned IS NULL
    `, [season2StartDate]);
    const sharedUpdated = (sharedResult as any).affectedRows;

    // Bulk update: shared = false → 3 points
    const [notSharedResult] = await this.mysqlConn!.execute(`
      UPDATE user_brand_votes
      SET pointsEarned = 3
      WHERE date < ? AND shared = 0 AND pointsEarned IS NULL
    `, [season2StartDate]);
    const notSharedUpdated = (notSharedResult as any).affectedRows;

    this.stats.season1Votes = sharedUpdated + notSharedUpdated;
    this.stats.season1Updated = sharedUpdated + notSharedUpdated;

    console.log(`   ✅ Updated ${sharedUpdated} shared votes → 6 points`);
    console.log(`   ✅ Updated ${notSharedUpdated} non-shared votes → 3 points`);
  }

  /**
   * Get only Season 2 votes that need backfilling
   */
  async getSeason2VotesToBackfill(): Promise<Vote[]> {
    console.log('\n📊 Fetching Season 2 votes to backfill...');

    const season2StartDate = SEASON_2_START_DATE.toISOString().slice(0, 19).replace('T', ' ');

    const [rows] = await this.mysqlConn!.execute(`
      SELECT
        v.transactionHash,
        v.date,
        v.shared,
        v.claimedAt,
        v.pointsEarned,
        u.fid as userFid
      FROM user_brand_votes v
      INNER JOIN users u ON v.userId = u.id
      WHERE v.date >= ? AND v.pointsEarned IS NULL
      ORDER BY v.date ASC
    `, [season2StartDate]);

    const votes = (rows as any[]).map((row) => ({
      transactionHash: row.transactionHash,
      date: new Date(row.date),
      shared: Boolean(row.shared),
      claimedAt: row.claimedAt ? new Date(row.claimedAt) : null,
      pointsEarned: row.pointsEarned,
      userFid: row.userFid,
    }));

    console.log(`   Found ${votes.length} Season 2 votes with NULL pointsEarned`);

    return votes;
  }

  /**
   * Main backfill logic
   */
  async backfill(): Promise<BackfillStats> {
    this.stats.startTime = new Date();

    console.log('\n' + '='.repeat(60));
    console.log('         POINTS EARNED BACKFILL');
    console.log('='.repeat(60));
    console.log(`Mode: ${this.isDryRun ? 'DRY RUN (no changes)' : 'LIVE (will update DB)'}`);
    console.log(`Season 2 start: ${SEASON_2_START_DATE.toISOString()}`);
    console.log('='.repeat(60));

    try {
      // STEP 1: Bulk update Season 1 votes (fast!)
      await this.bulkUpdateSeason1();

      // STEP 2: Preload level-ups for Season 2 lookups
      await this.preloadLevelUps();

      // STEP 3: Get only Season 2 votes
      const votes = await this.getSeason2VotesToBackfill();
      this.stats.season2Votes = votes.length;
      this.stats.totalVotes = this.stats.season1Votes + votes.length;

      if (votes.length === 0) {
        console.log('\n✅ No Season 2 votes need backfilling!');
        this.stats.endTime = new Date();
        return this.stats;
      }

      console.log('\n📝 Processing Season 2 votes (one-by-one for level lookup)...');

      // Process Season 2 votes
      const BATCH_SIZE = 100;
      for (let i = 0; i < votes.length; i += BATCH_SIZE) {
        const batch = votes.slice(i, i + BATCH_SIZE);

        for (const vote of batch) {
          try {
            // Calculate points for Season 2
            let pointsEarned: number;
            if (!vote.claimedAt) {
              // Unclaimed: just voting points
              pointsEarned = 3;
              this.stats.season2Unclaimed++;
            } else {
              // Claimed: 3 + (brndPowerLevel at claim time × 3)
              const levelAtClaim = this.getBrndPowerLevelAtTime(vote.userFid, vote.claimedAt);
              pointsEarned = 3 + (levelAtClaim * 3);
              this.stats.season2Claimed++;

              // Log interesting cases (level > 0)
              if (levelAtClaim > 0) {
                console.log(
                  `\n   FID ${vote.userFid}: claimed at level ${levelAtClaim} → ${pointsEarned} points`,
                );
              }
            }

            // Update the database
            await this.updateVotePoints(vote.transactionHash, pointsEarned);
            this.stats.season2Updated++;
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            this.stats.errors.push(`${vote.transactionHash}: ${msg}`);
          }
        }

        // Progress log
        const progress = Math.min(i + BATCH_SIZE, votes.length);
        const pct = Math.round((progress / votes.length) * 100);
        process.stdout.write(
          `\r   Progress: ${progress}/${votes.length} (${pct}%) - Claimed: ${this.stats.season2Claimed}, Unclaimed: ${this.stats.season2Unclaimed}`,
        );
      }

      console.log('\n');
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.stats.errors.push(`Fatal: ${msg}`);
      console.error('\n❌ Backfill error:', error);
    }

    this.stats.endTime = new Date();
    return this.stats;
  }

  /**
   * Print summary of the backfill operation
   */
  printSummary(): void {
    const duration = this.stats.endTime
      ? Math.round(
          (this.stats.endTime.getTime() - this.stats.startTime.getTime()) / 1000,
        )
      : 0;

    console.log('='.repeat(60));
    console.log('         BACKFILL SUMMARY');
    console.log('='.repeat(60));
    console.log(`Mode:                 ${this.isDryRun ? 'DRY RUN' : 'LIVE'}`);
    console.log(`Duration:             ${duration}s`);
    console.log('');
    console.log(`Total votes:          ${this.stats.totalVotes}`);
    console.log('');
    console.log('SEASON 1 (shared-based):');
    console.log(`  Votes processed:    ${this.stats.season1Votes}`);
    console.log(`  Updated:            ${this.stats.season1Updated}`);
    console.log('');
    console.log('SEASON 2 (level-based):');
    console.log(`  Votes processed:    ${this.stats.season2Votes}`);
    console.log(`  Unclaimed (3 pts):  ${this.stats.season2Unclaimed}`);
    console.log(`  Claimed:            ${this.stats.season2Claimed}`);
    console.log(`  Updated:            ${this.stats.season2Updated}`);
    console.log('');
    console.log(`Errors:               ${this.stats.errors.length}`);
    console.log('='.repeat(60));

    if (this.stats.errors.length > 0) {
      console.log('\n⚠️ Errors:');
      this.stats.errors.slice(0, 10).forEach((err, i) => {
        console.log(`   ${i + 1}. ${err}`);
      });
      if (this.stats.errors.length > 10) {
        console.log(`   ... and ${this.stats.errors.length - 10} more`);
      }
    }

    if (this.isDryRun) {
      console.log('\n⚠️ This was a DRY RUN. No changes were made.');
      console.log('   Run without --dry-run to apply changes.');
    } else {
      console.log('\n✅ Backfill complete!');
    }
  }
}

// ============================================================================
// Recalculate All Mode
// ============================================================================

class FullRecalculator extends PointsBackfiller {
  /**
   * Bulk update ALL Season 1 votes (not just NULL)
   */
  async bulkUpdateSeason1(): Promise<void> {
    console.log('\n📝 Recalculating ALL Season 1 votes (bulk update)...');

    const season2StartDate = SEASON_2_START_DATE.toISOString().slice(0, 19).replace('T', ' ');

    if (this.isDryRun) {
      const [sharedRows] = await this.mysqlConn!.execute(`
        SELECT COUNT(*) as cnt FROM user_brand_votes
        WHERE date < ? AND shared = 1
      `, [season2StartDate]);
      const [notSharedRows] = await this.mysqlConn!.execute(`
        SELECT COUNT(*) as cnt FROM user_brand_votes
        WHERE date < ? AND shared = 0
      `, [season2StartDate]);

      const sharedCount = (sharedRows as any[])[0].cnt;
      const notSharedCount = (notSharedRows as any[])[0].cnt;

      this.stats.season1Votes = sharedCount + notSharedCount;
      this.stats.season1Updated = sharedCount + notSharedCount;
      console.log(`   Would update ${sharedCount} shared votes → 6 points`);
      console.log(`   Would update ${notSharedCount} non-shared votes → 3 points`);
      return;
    }

    // Bulk update ALL: shared = true → 6 points
    const [sharedResult] = await this.mysqlConn!.execute(`
      UPDATE user_brand_votes
      SET pointsEarned = 6
      WHERE date < ? AND shared = 1
    `, [season2StartDate]);
    const sharedUpdated = (sharedResult as any).affectedRows;

    // Bulk update ALL: shared = false → 3 points
    const [notSharedResult] = await this.mysqlConn!.execute(`
      UPDATE user_brand_votes
      SET pointsEarned = 3
      WHERE date < ? AND shared = 0
    `, [season2StartDate]);
    const notSharedUpdated = (notSharedResult as any).affectedRows;

    this.stats.season1Votes = sharedUpdated + notSharedUpdated;
    this.stats.season1Updated = sharedUpdated + notSharedUpdated;

    console.log(`   ✅ Updated ${sharedUpdated} shared votes → 6 points`);
    console.log(`   ✅ Updated ${notSharedUpdated} non-shared votes → 3 points`);
  }

  /**
   * Get ALL Season 2 votes (not just NULL pointsEarned)
   */
  async getSeason2VotesToBackfill(): Promise<Vote[]> {
    console.log('\n📊 Fetching ALL Season 2 votes for recalculation...');

    const season2StartDate = SEASON_2_START_DATE.toISOString().slice(0, 19).replace('T', ' ');

    const [rows] = await this.mysqlConn!.execute(`
      SELECT
        v.transactionHash,
        v.date,
        v.shared,
        v.claimedAt,
        v.pointsEarned,
        u.fid as userFid
      FROM user_brand_votes v
      INNER JOIN users u ON v.userId = u.id
      WHERE v.date >= ?
      ORDER BY v.date ASC
    `, [season2StartDate]);

    const votes = (rows as any[]).map((row) => ({
      transactionHash: row.transactionHash,
      date: new Date(row.date),
      shared: Boolean(row.shared),
      claimedAt: row.claimedAt ? new Date(row.claimedAt) : null,
      pointsEarned: row.pointsEarned,
      userFid: row.userFid,
    }));

    console.log(`   Found ${votes.length} total Season 2 votes`);

    return votes;
  }
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run');
  const recalcAll = args.includes('--recalc-all');

  if (args.includes('--help')) {
    console.log(`
Points Earned Backfill Script

Usage:
  bun run scripts/backfill-points-earned.ts [options]

Options:
  --dry-run      Show what would be updated without making changes
  --recalc-all   Recalculate ALL votes (not just NULL pointsEarned)
  --help         Show this help message

Examples:
  bun run scripts/backfill-points-earned.ts --dry-run
  bun run scripts/backfill-points-earned.ts
  bun run scripts/backfill-points-earned.ts --recalc-all
`);
    process.exit(0);
  }

  const backfiller = recalcAll
    ? new FullRecalculator(isDryRun)
    : new PointsBackfiller(isDryRun);

  try {
    await backfiller.connect();
    await backfiller.backfill();
    backfiller.printSummary();
  } catch (error) {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await backfiller.disconnect();
  }
}

main();
