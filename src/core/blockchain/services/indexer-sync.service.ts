import { Injectable, Logger, forwardRef, Inject } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In, DataSource } from 'typeorm';
import { Client } from 'pg';

import { User, UserBrandVotes, Brand, Category } from '../../../models';
import { UserService } from '../../user/services/user.service';
import { BlockchainService } from './blockchain.service';

export interface SyncStats {
  usersChecked: number;
  usersUpdated: number;
  votesChecked: number;
  votesInserted: number;
  votesUpdated: number;
  brandsChecked: number;
  brandsCreated: number;
  brandsUpdated: number;
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
  /** Whether to sync brands. Default: true */
  syncBrands?: boolean;
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
    @InjectRepository(Category)
    private readonly categoryRepository: Repository<Category>,
    @Inject(forwardRef(() => UserService))
    private readonly userService: UserService,
    @Inject(forwardRef(() => BlockchainService))
    private readonly blockchainService: BlockchainService,
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
      syncBrands = true,
    } = options;

    const stats: SyncStats = {
      usersChecked: 0,
      usersUpdated: 0,
      votesChecked: 0,
      votesInserted: 0,
      votesUpdated: 0,
      brandsChecked: 0,
      brandsCreated: 0,
      brandsUpdated: 0,
      errors: [],
      startTime: new Date(),
    };

    const isFullSync = windowHours === 0;
    this.logger.log(
      `Starting indexer sync (${isFullSync ? 'FULL SYNC' : `last ${windowHours}h`})`,
    );

    try {
      await this.connectToIndexer();

      // Sync brands FIRST so votes have valid brand references
      if (syncBrands) {
        await this.syncBrands(stats, windowHours);
      }

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

        // Only update if indexer level is HIGHER (power levels only go up)
        if (indexerLevel > mysqlUser.brndPowerLevel) {
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
   * Sync brands from indexer to MySQL
   */
  private async syncBrands(stats: SyncStats, windowHours: number): Promise<void> {
    this.logger.log('Syncing brands...');
    const schema = this.getSchema();

    try {
      // Build query - for brands we typically want full sync since they're not time-windowed
      // But we can use block_number for incremental syncs
      let query: string;
      const isFullSync = windowHours === 0;

      if (isFullSync) {
        query = `
          SELECT id, fid, wallet_address, handle, metadata_hash, total_brnd_awarded,
                 available_brnd, created_at, block_number, transaction_hash
          FROM "${schema}".brands
          ORDER BY id ASC
        `;
      } else {
        // For time-windowed sync, get brands created in the window
        // Using created_at (unix timestamp) for filtering
        const windowStart = Math.floor(
          (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
        );
        query = `
          SELECT id, fid, wallet_address, handle, metadata_hash, total_brnd_awarded,
                 available_brnd, created_at, block_number, transaction_hash
          FROM "${schema}".brands
          WHERE created_at >= ${windowStart}
          ORDER BY id ASC
        `;
      }

      const result = await this.indexerClient!.query(query);
      const indexerBrands = result.rows;

      this.logger.log(`Found ${indexerBrands.length} brands to check in indexer`);
      stats.brandsChecked = indexerBrands.length;

      if (indexerBrands.length === 0) {
        this.logger.log('No brands to sync');
        return;
      }

      // Get existing brands from MySQL by onChainId
      const onChainIds = indexerBrands.map((b: any) => b.id);
      const existingBrands = await this.brandRepository.find({
        where: { onChainId: In(onChainIds) },
        select: ['id', 'onChainId', 'metadataHash'],
      });
      const existingBrandMap = new Map(
        existingBrands.map((b) => [b.onChainId, b]),
      );

      // Get or create default category
      let defaultCategory = await this.categoryRepository.findOne({
        where: { name: 'General' },
      });
      if (!defaultCategory) {
        defaultCategory = await this.categoryRepository.save(
          this.categoryRepository.create({ name: 'General' }),
        );
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
            // Brand exists - check if metadata hash changed (needs update)
            if (existingBrand.metadataHash !== metadataHash && metadataHash) {
              this.logger.log(
                `Updating brand onChainId ${onChainId}: metadata changed`,
              );

              // Fetch new metadata from IPFS
              let metadata: any = {};
              try {
                metadata = await this.blockchainService.fetchMetadataFromIpfs(
                  metadataHash,
                );
              } catch (ipfsError) {
                this.logger.warn(
                  `Failed to fetch IPFS metadata for brand ${onChainId}: ${ipfsError}`,
                );
              }

              // Update the brand
              await this.brandRepository.update(
                { onChainId },
                {
                  metadataHash,
                  onChainHandle: indexerBrand.handle,
                  onChainFid: indexerBrand.fid,
                  onChainWalletAddress: indexerBrand.wallet_address,
                  totalBrndAwarded: indexerBrand.total_brnd_awarded?.toString() || '0',
                  availableBrnd: indexerBrand.available_brnd?.toString() || '0',
                  ...(metadata.name && { name: metadata.name }),
                  ...(metadata.description && { description: metadata.description }),
                  ...(metadata.imageUrl && { imageUrl: metadata.imageUrl }),
                  ...(metadata.url && { url: metadata.url }),
                  ...(metadata.profile && { profile: metadata.profile }),
                  ...(metadata.channel && { channel: metadata.channel }),
                },
              );
              stats.brandsUpdated++;
            }
            continue;
          }

          // Brand doesn't exist - create it
          this.logger.log(
            `Creating brand onChainId ${onChainId}: ${indexerBrand.handle}`,
          );

          // Fetch metadata from IPFS
          let metadata: any = {};
          if (metadataHash) {
            try {
              metadata = await this.blockchainService.fetchMetadataFromIpfs(
                metadataHash,
              );
              this.logger.log(
                `Fetched IPFS metadata for brand ${onChainId}:`,
                metadata,
              );
            } catch (ipfsError) {
              this.logger.warn(
                `Failed to fetch IPFS metadata for brand ${onChainId}, using handle as name: ${ipfsError}`,
              );
            }
          }

          // Determine profile/channel from metadata or handle
          let profile = metadata.profile || '';
          let channel = metadata.channel || '';
          let queryType = metadata.queryType ?? 0;

          // If no profile/channel in metadata, use handle
          if (!profile && !channel) {
            // Default to channel with handle
            channel = `/${indexerBrand.handle}`;
            queryType = 0;
          }

          // Create the brand
          const newBrand = this.brandRepository.create({
            // On-chain data
            onChainId: indexerBrand.id,
            onChainHandle: indexerBrand.handle,
            onChainFid: indexerBrand.fid,
            onChainWalletAddress: indexerBrand.wallet_address,
            onChainCreatedAt: new Date(Number(indexerBrand.created_at) * 1000),
            metadataHash,

            // Metadata from IPFS
            name: metadata.name || indexerBrand.handle,
            url: metadata.url || '',
            warpcastUrl: metadata.warpcastUrl || metadata.url || '',
            description: metadata.description || '',
            imageUrl: metadata.imageUrl || '',
            profile,
            channel,
            queryType,
            followerCount: metadata.followerCount || 0,
            category: defaultCategory,

            // Initialize scoring fields
            score: 0,
            stateScore: 0,
            scoreDay: 0,
            stateScoreDay: 0,
            scoreWeek: 0,
            stateScoreWeek: 0,
            scoreMonth: 0,
            stateScoreMonth: 0,
            ranking: '0',
            rankingWeek: 0,
            rankingMonth: 0,
            bonusPoints: 0,
            banned: 0,
            currentRanking: 0,

            // Blockchain amounts
            totalBrndAwarded: indexerBrand.total_brnd_awarded?.toString() || '0',
            availableBrnd: indexerBrand.available_brnd?.toString() || '0',
          });

          await this.brandRepository.save(newBrand);
          stats.brandsCreated++;

          this.logger.log(
            `Created brand: ${newBrand.name} (onChainId: ${onChainId})`,
          );
        } catch (error) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);
          stats.errors.push(
            `Brand ${indexerBrand.id} (${indexerBrand.handle}): ${errorMessage}`,
          );
          this.logger.error(
            `Error syncing brand ${indexerBrand.id}:`,
            error,
          );
        }
      }

      this.logger.log(
        `Brand sync complete: ${stats.brandsCreated} created, ${stats.brandsUpdated} updated`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      stats.errors.push(`Brand sync error: ${errorMessage}`);
      this.logger.error('Error syncing brands:', error);
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
    this.logger.log(`Brands checked:     ${stats.brandsChecked}`);
    this.logger.log(`Brands created:     ${stats.brandsCreated}`);
    this.logger.log(`Brands updated:     ${stats.brandsUpdated}`);
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
