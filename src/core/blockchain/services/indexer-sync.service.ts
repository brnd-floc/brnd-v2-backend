import { Injectable, Logger, forwardRef, Inject } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, DataSource } from 'typeorm';
import { Client } from 'pg';

import { User, UserBrandVotes, Brand } from '../../../models';
import { UserService } from '../../user/services/user.service';

export interface SyncStats {
  usersChecked: number;
  usersUpdated: number;
  votesChecked: number;
  votesInserted: number;
  votesUpdated: number;
  errors: string[];
  startTime: Date;
  endTime?: Date;
}

export interface SyncOptions {
  /** Sync window in hours. Use 0 for full sync. Default: 48 */
  windowHours?: number;
  /** Whether to sync user power levels. Default: true */
  syncPowerLevels?: boolean;
  /** Whether to sync votes. Default: true */
  syncVotes?: boolean;
}

@Injectable()
export class IndexerSyncService {
  private readonly logger = new Logger(IndexerSyncService.name);
  private indexerClient: Client | null = null;

  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    @Inject(forwardRef(() => UserService))
    private readonly userService: UserService,
  ) {}

  /**
   * Connect to the PostgreSQL indexer database
   */
  private async connectToIndexer(): Promise<Client> {
    if (this.indexerClient) {
      return this.indexerClient;
    }

    const connectionString = process.env.INDEXER_DB_URL;

    if (!connectionString) {
      throw new Error('INDEXER_DB_URL environment variable is not set');
    }

    this.indexerClient = new Client({ connectionString });
    await this.indexerClient.connect();
    this.logger.log('Connected to PostgreSQL indexer database');

    return this.indexerClient;
  }

  /**
   * Disconnect from the indexer database
   */
  private async disconnectFromIndexer(): Promise<void> {
    if (this.indexerClient) {
      await this.indexerClient.end();
      this.indexerClient = null;
      this.logger.log('Disconnected from PostgreSQL indexer database');
    }
  }

  /**
   * Get the indexer schema name
   */
  private getSchema(): string {
    return process.env.INDEXER_DB_SCHEMA || 'public';
  }

  /**
   * Main sync method - syncs data from indexer to MySQL
   */
  async sync(options: SyncOptions = {}): Promise<SyncStats> {
    const {
      windowHours = 48,
      syncPowerLevels = true,
      syncVotes = true,
    } = options;

    const stats: SyncStats = {
      usersChecked: 0,
      usersUpdated: 0,
      votesChecked: 0,
      votesInserted: 0,
      votesUpdated: 0,
      errors: [],
      startTime: new Date(),
    };

    const isFullSync = windowHours === 0;
    this.logger.log(
      `Starting indexer sync (${isFullSync ? 'FULL SYNC' : `last ${windowHours}h`})`,
    );

    try {
      await this.connectToIndexer();

      if (syncPowerLevels) {
        await this.syncUserPowerLevels(stats, windowHours);
      }

      if (syncVotes) {
        await this.syncVotes(stats, windowHours);
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      stats.errors.push(`Fatal error: ${errorMessage}`);
      this.logger.error('Sync failed:', error);
    } finally {
      await this.disconnectFromIndexer();
      stats.endTime = new Date();
    }

    this.logSyncSummary(stats);
    return stats;
  }

  /**
   * Sync user power levels from indexer to MySQL
   */
  private async syncUserPowerLevels(
    stats: SyncStats,
    windowHours: number,
  ): Promise<void> {
    this.logger.log('Syncing user power levels...');
    const schema = this.getSchema();

    try {
      // Build query based on window
      let query: string;
      const isFullSync = windowHours === 0;

      if (isFullSync) {
        // Full sync - get all users with power level > 0
        query = `
          SELECT fid, brnd_power_level
          FROM "${schema}".users
          WHERE brnd_power_level > 0
        `;
      } else {
        // Get users who leveled up in the time window
        const windowStart = Math.floor(
          (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
        );
        query = `
          SELECT DISTINCT u.fid, u.brnd_power_level
          FROM "${schema}".users u
          INNER JOIN "${schema}".brnd_power_level_ups lup ON u.fid = lup.fid
          WHERE lup.timestamp >= ${windowStart}
        `;
      }

      const result = await this.indexerClient!.query(query);
      const indexerUsers = result.rows;

      this.logger.log(`Found ${indexerUsers.length} users to check in indexer`);
      stats.usersChecked = indexerUsers.length;

      if (indexerUsers.length === 0) {
        this.logger.log('No users to sync');
        return;
      }

      // Get FIDs to lookup in MySQL
      const fids = indexerUsers.map((u: any) => u.fid);

      // Fetch corresponding MySQL users
      const mysqlUsers = await this.userRepository.find({
        where: { fid: In(fids) },
        select: ['id', 'fid', 'brndPowerLevel'],
      });

      const mysqlUserMap = new Map(mysqlUsers.map((u) => [u.fid, u]));

      // Process each user
      for (const indexerUser of indexerUsers) {
        const fid = indexerUser.fid;
        const indexerLevel = indexerUser.brnd_power_level;
        const mysqlUser = mysqlUserMap.get(fid);

        if (!mysqlUser) {
          this.logger.warn(`User FID ${fid} not found in MySQL, skipping`);
          continue;
        }

        if (mysqlUser.brndPowerLevel !== indexerLevel) {
          this.logger.log(
            `Updating user FID ${fid}: brndPowerLevel ${mysqlUser.brndPowerLevel} -> ${indexerLevel}`,
          );

          await this.userRepository.update(
            { fid },
            { brndPowerLevel: indexerLevel },
          );
          stats.usersUpdated++;
        }
      }

      this.logger.log(
        `Power level sync complete: ${stats.usersUpdated} users updated`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      stats.errors.push(`Power level sync error: ${errorMessage}`);
      this.logger.error('Error syncing power levels:', error);
    }
  }

  /**
   * Sync votes from indexer to MySQL
   */
  private async syncVotes(stats: SyncStats, windowHours: number): Promise<void> {
    this.logger.log('Syncing votes...');
    const schema = this.getSchema();

    try {
      // Build query based on window
      let query: string;
      const isFullSync = windowHours === 0;

      if (isFullSync) {
        query = `
          SELECT id, voter, fid, day, brand_ids, cost, block_number, transaction_hash, timestamp
          FROM "${schema}".votes
          ORDER BY timestamp ASC
        `;
      } else {
        const windowStart = Math.floor(
          (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
        );
        query = `
          SELECT id, voter, fid, day, brand_ids, cost, block_number, transaction_hash, timestamp
          FROM "${schema}".votes
          WHERE timestamp >= ${windowStart}
          ORDER BY timestamp ASC
        `;
      }

      const result = await this.indexerClient!.query(query);
      const indexerVotes = result.rows;

      this.logger.log(`Found ${indexerVotes.length} votes to check in indexer`);
      stats.votesChecked = indexerVotes.length;

      if (indexerVotes.length === 0) {
        this.logger.log('No votes to sync');
        return;
      }

      // Get existing votes from MySQL by transaction hash
      const txHashes = indexerVotes.map((v: any) => v.transaction_hash);
      const existingVotes = await this.userBrandVotesRepository.find({
        where: { transactionHash: In(txHashes) },
        select: ['transactionHash'],
      });
      const existingTxSet = new Set(existingVotes.map((v) => v.transactionHash));

      // Preload user and brand caches
      const fids = [...new Set(indexerVotes.map((v: any) => v.fid))];
      const mysqlUsers = await this.userRepository.find({
        where: { fid: In(fids) },
        select: ['id', 'fid'],
      });
      const userFidToId = new Map(mysqlUsers.map((u) => [u.fid, u.id]));

      const brands = await this.brandRepository.find({ select: ['id'] });
      const brandIds = new Set(brands.map((b) => b.id));

      // Process votes in batches
      const BATCH_SIZE = 50;
      for (let i = 0; i < indexerVotes.length; i += BATCH_SIZE) {
        const batch = indexerVotes.slice(i, i + BATCH_SIZE);

        for (const indexerVote of batch) {
          try {
            const txHash = indexerVote.transaction_hash;

            if (existingTxSet.has(txHash)) {
              // Vote exists, skip (we trust indexer data but don't overwrite claim data)
              continue;
            }

            // Parse brand IDs
            let brandIdsArray: number[];
            try {
              brandIdsArray = JSON.parse(indexerVote.brand_ids);
            } catch {
              this.logger.warn(
                `Invalid brand_ids for vote ${txHash}, skipping`,
              );
              continue;
            }

            if (brandIdsArray.length !== 3) {
              this.logger.warn(
                `Vote ${txHash} has ${brandIdsArray.length} brands, expected 3, skipping`,
              );
              continue;
            }

            // Check if all brands exist
            const missingBrands = brandIdsArray.filter((id) => !brandIds.has(id));
            if (missingBrands.length > 0) {
              this.logger.warn(
                `Vote ${txHash} has missing brands: ${missingBrands.join(', ')}, skipping`,
              );
              continue;
            }

            // Get or create user
            let userId = userFidToId.get(indexerVote.fid);
            if (!userId) {
              // Create minimal user record
              const newUser = await this.createMinimalUser(
                indexerVote.fid,
                indexerVote.voter,
              );
              if (newUser) {
                userId = newUser.id;
                userFidToId.set(indexerVote.fid, userId);
              } else {
                this.logger.warn(
                  `Could not create user for FID ${indexerVote.fid}, skipping vote`,
                );
                continue;
              }
            }

            // Calculate vote data
            const voteDate = new Date(Number(indexerVote.timestamp) * 1000);
            const day = Math.floor(Number(indexerVote.timestamp) / 86400);
            const costBigInt = BigInt(indexerVote.cost);
            const brndPaid = Number(costBigInt / BigInt(10 ** 18));
            const rewardAmount = (costBigInt * 10n).toString();

            // Insert the vote using raw query for simplicity with foreign keys
            await this.userBrandVotesRepository.query(
              `INSERT INTO user_brand_votes
               (transactionHash, id, userId, brand1Id, brand2Id, brand3Id, date, day,
                brndPaidWhenCreatingPodium, rewardAmount, shared, shareVerified, pointsEarned)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              [
                txHash,
                indexerVote.id,
                userId,
                brandIdsArray[0],
                brandIdsArray[1],
                brandIdsArray[2],
                voteDate,
                day,
                brndPaid,
                rewardAmount,
                false,
                false,
                3, // Initial 3 points for voting
              ],
            );

            stats.votesInserted++;
            existingTxSet.add(txHash); // Prevent duplicate inserts in same run

            // Add 3 points for the vote to user's points and totalS2Points
            try {
              await this.userService.addPoints(userId, 3);
              this.logger.debug(`Added 3 points for synced vote to user ${userId}`);
            } catch (pointsError) {
              this.logger.warn(`Failed to add points for vote ${txHash}: ${pointsError}`);
            }
          } catch (error) {
            const errorMessage =
              error instanceof Error ? error.message : String(error);
            stats.errors.push(
              `Vote ${indexerVote.transaction_hash}: ${errorMessage}`,
            );
          }
        }

        // Log progress
        const progress = Math.min(i + BATCH_SIZE, indexerVotes.length);
        this.logger.log(
          `Processed ${progress}/${indexerVotes.length} votes (${stats.votesInserted} inserted)`,
        );
      }

      this.logger.log(
        `Vote sync complete: ${stats.votesInserted} votes inserted`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      stats.errors.push(`Vote sync error: ${errorMessage}`);
      this.logger.error('Error syncing votes:', error);
    }
  }

  /**
   * Create a minimal user record for a new FID
   */
  private async createMinimalUser(
    fid: number,
    voterAddress: string,
  ): Promise<User | null> {
    try {
      const user = this.userRepository.create({
        fid,
        username: `user_${fid}`,
        photoUrl: '',
        address: voterAddress,
        points: 0,
        totalS1Points: 0,
        totalS2Points: 0,
        dailyStreak: 0,
        maxDailyStreak: 0,
        totalPodiums: 0,
        votedBrandsCount: 0,
        brndPowerLevel: 0,
        totalVotes: 0,
        banned: false,
        powerups: 0,
        verified: false,
        notificationsEnabled: false,
        neynarScore: 0,
      });

      return await this.userRepository.save(user);
    } catch (error) {
      this.logger.error(`Error creating user for FID ${fid}:`, error);
      return null;
    }
  }

  /**
   * Log a summary of the sync operation
   */
  private logSyncSummary(stats: SyncStats): void {
    const duration = stats.endTime
      ? Math.round((stats.endTime.getTime() - stats.startTime.getTime()) / 1000)
      : 0;

    this.logger.log('========================================');
    this.logger.log('         INDEXER SYNC SUMMARY           ');
    this.logger.log('========================================');
    this.logger.log(`Duration:           ${duration}s`);
    this.logger.log(`Users checked:      ${stats.usersChecked}`);
    this.logger.log(`Users updated:      ${stats.usersUpdated}`);
    this.logger.log(`Votes checked:      ${stats.votesChecked}`);
    this.logger.log(`Votes inserted:     ${stats.votesInserted}`);
    this.logger.log(`Errors:             ${stats.errors.length}`);
    this.logger.log('========================================');

    if (stats.errors.length > 0) {
      this.logger.warn('Errors encountered:');
      stats.errors.slice(0, 10).forEach((err, i) => {
        this.logger.warn(`  ${i + 1}. ${err}`);
      });
      if (stats.errors.length > 10) {
        this.logger.warn(`  ... and ${stats.errors.length - 10} more`);
      }
    }
  }
}
