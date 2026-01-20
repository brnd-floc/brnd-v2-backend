import { Injectable } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, MoreThan } from 'typeorm';

import { User, UserBrandVotes } from '../../models';
import { UserService } from '../user/services';
import { AirdropService } from '../airdrop/services/airdrop.service';
import { IndexerSyncService } from '../blockchain/services/indexer-sync.service';
import { logger } from '../../main';

@Injectable()
export class DailyService {
  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
    private readonly userService: UserService,
    private readonly airdropService: AirdropService,
    private readonly indexerSyncService: IndexerSyncService,
  ) {}

  /**
   * Daily reset job that runs at 00:00 UTC every day
   * - Resets daily voting state
   * - Updates user streaks (resets if user didn't vote in last 24 hours)
   * - Refreshes leaderboard cache
   */
  @Cron('0 0 * * *', {
    name: 'dailyReset',
    timeZone: 'UTC',
  })
  async handleDailyReset(): Promise<void> {
    console.log(
      '🌅 [CRON] Daily reset cron job triggered at:',
      new Date().toISOString(),
    );
    logger.log('🌅 [DAILY] Starting daily reset at 00:00 UTC');

    try {
      await Promise.all([
        this.resetUserStreaks(),
        this.refreshLeaderboard(),
        this.calculateDailyAirdropLeaderboard(),
      ]);

      logger.log('✅ [DAILY] Daily reset completed successfully');
    } catch (error) {
      logger.error('❌ [DAILY] Error during daily reset:', error);
      // Don't throw - we want the cron to continue running
    }
  }

  /**
   * Reset user streaks for users who didn't vote in the last 24 hours
   */
  private async resetUserStreaks(): Promise<void> {
    logger.log('🔄 [DAILY] Processing user streak resets...');

    try {
      // Calculate 24 hours ago
      const twentyFourHoursAgo = new Date();
      twentyFourHoursAgo.setHours(twentyFourHoursAgo.getHours() - 24);

      // Get all users who have a streak > 0
      const usersWithStreaks = await this.userRepository.find({
        where: {
          dailyStreak: MoreThan(0), // Users with active streaks
        },
        select: ['id', 'fid', 'dailyStreak', 'lastVoteTimestamp'],
      });

      let streaksReset = 0;
      const batchSize = 50; // Process in batches to avoid overwhelming the database

      for (let i = 0; i < usersWithStreaks.length; i += batchSize) {
        const batch = usersWithStreaks.slice(i, i + batchSize);

        const resetPromises = batch.map(async (user) => {
          // Check if user voted in the last 24 hours
          if (
            !user.lastVoteTimestamp ||
            user.lastVoteTimestamp < twentyFourHoursAgo
          ) {
            // User didn't vote in last 24 hours - reset streak
            await this.userRepository.update(user.id, {
              dailyStreak: 0,
            });
            streaksReset++;
            logger.log(
              `📉 [DAILY] Reset streak for user FID ${user.fid} (was ${user.dailyStreak})`,
            );
          }
        });

        await Promise.all(resetPromises);
      }

      logger.log(
        `✅ [DAILY] Processed ${usersWithStreaks.length} users, reset ${streaksReset} streaks`,
      );
    } catch (error) {
      logger.error('❌ [DAILY] Error resetting user streaks:', error);
      throw error;
    }
  }

  /**
   * Refresh the leaderboard cache to start the new day with fresh data
   */
  private async refreshLeaderboard(): Promise<void> {
    logger.log('🏆 [DAILY] Refreshing leaderboard cache...');

    try {
      // Invalidate the current cache to force refresh
      this.userService.invalidateLeaderboardCache();

      // Trigger a fresh leaderboard calculation (this will rebuild the cache)
      await this.userService.getLeaderboard(1, 10);

      logger.log('✅ [DAILY] Leaderboard cache refreshed');
    } catch (error) {
      logger.error('❌ [DAILY] Error refreshing leaderboard:', error);
      throw error;
    }
  }

  /**
   * Calculate daily airdrop leaderboard for top 1111 users
   * This runs as part of the daily reset to update positions and token allocations
   */
  private async calculateDailyAirdropLeaderboard(): Promise<void> {
    logger.log('🏆 [DAILY] Starting daily airdrop leaderboard calculation...');

    try {
      // Check if snapshot already exists - if so, skip calculations to preserve frozen allocations
      const existingSnapshotsCount =
        await this.airdropService.airdropSnapshotRepository.count();

      if (existingSnapshotsCount > 0) {
        logger.log(
          `⚠️ [DAILY] Found ${existingSnapshotsCount} existing snapshot(s) - skipping airdrop calculations to preserve frozen allocations`,
        );
        logger.log(
          'ℹ️ [DAILY] Airdrop allocations dont happen anymore because the snapshot for the airdrop was taken.',
        );
        return;
      }

      logger.log(
        '✅ [DAILY] No snapshots found - proceeding with airdrop calculations...',
      );
      const startTime = Date.now();

      // Calculate airdrop for all eligible users (top 1111 by points)
      const result = await this.airdropService.calculateAirdropForAllUsers(88); // Use 0 batch size for daily run because the snapshot for the airdrop was taken.

      const endTime = Date.now();
      const duration = Math.round((endTime - startTime) / 1000);

      logger.log('✅ [DAILY] Airdrop leaderboard calculation completed', {
        duration: `${duration}s`,
        eligibleUsers: result.eligibleUsers,
        processed: result.processed,
        successful: result.successful,
        failed: result.failed,
        totalTokensAllocated: result.totalTokensAllocated,
        totalAirdropPoints: result.totalAirdropPoints,
      });

      if (result.failed > 0) {
        logger.warn(
          `⚠️ [DAILY] ${result.failed} users failed during airdrop calculation`,
          {
            errors: result.errors.slice(0, 5), // Log first 5 errors
          },
        );
      }
    } catch (error) {
      logger.error(
        '❌ [DAILY] Error calculating daily airdrop leaderboard:',
        error,
      );
      throw error;
    }
  }

  /**
   * Manual trigger for daily reset (useful for testing or emergency resets)
   */
  async triggerManualReset(): Promise<void> {
    logger.log('🔧 [DAILY] Manual reset triggered');
    await this.handleDailyReset();
  }

  /**
   * Manual trigger for airdrop leaderboard calculation
   */
  async triggerManualAirdropCalculation(): Promise<any> {
    logger.log('🔧 [DAILY] Manual airdrop calculation triggered');

    try {
      // Check if snapshot already exists - if so, warn but allow manual override
      const existingSnapshotsCount =
        await this.airdropService.airdropSnapshotRepository.count();

      if (existingSnapshotsCount > 0) {
        logger.warn(
          `⚠️ [DAILY] WARNING: ${existingSnapshotsCount} existing snapshot(s) found!`,
        );
        logger.warn(
          '⚠️ [DAILY] Manual calculation will proceed but may overwrite frozen allocations.',
        );
        logger.warn(
          'ℹ️ [DAILY] Consider clearing snapshots first if this is intentional.',
        );
      }

      const result = await this.airdropService.calculateAirdropForAllUsers(10); // Smaller batch for manual trigger
      logger.log(
        '✅ [DAILY] Manual airdrop calculation completed successfully',
      );
      return result;
    } catch (error) {
      logger.error('❌ [DAILY] Manual airdrop calculation failed:', error);
      throw error;
    }
  }

  /**
   * Daily indexer sync job that runs at 01:00 UTC every day
   * Syncs user power levels and votes from the PostgreSQL indexer to MySQL
   * Uses a 48-hour window to catch any missed data with a 1-day buffer
   */
  @Cron('0 1 * * *', {
    name: 'indexerSync',
    timeZone: 'UTC',
  })
  async handleIndexerSync(): Promise<void> {
    console.log(
      '🔄 [CRON] Indexer sync cron job triggered at:',
      new Date().toISOString(),
    );
    logger.log('🔄 [DAILY] Starting daily indexer sync at 01:00 UTC');

    try {
      const stats = await this.indexerSyncService.sync({
        windowHours: 48, // 48-hour window for daily sync
        syncPowerLevels: true,
        syncVotes: true,
      });

      logger.log('✅ [DAILY] Indexer sync completed', {
        usersUpdated: stats.usersUpdated,
        votesInserted: stats.votesInserted,
        errors: stats.errors.length,
      });

      if (stats.errors.length > 0) {
        logger.warn(
          `⚠️ [DAILY] ${stats.errors.length} errors during indexer sync`,
        );
      }
    } catch (error) {
      logger.error('❌ [DAILY] Error during indexer sync:', error);
      // Don't throw - we want the cron to continue running
    }
  }

  /**
   * Manual trigger for indexer sync with custom window
   * @param windowHours - Number of hours to look back (0 for full sync)
   */
  async triggerManualIndexerSync(windowHours: number = 48): Promise<any> {
    logger.log(
      `🔧 [DAILY] Manual indexer sync triggered (window: ${windowHours === 0 ? 'FULL' : windowHours + 'h'})`,
    );

    try {
      const stats = await this.indexerSyncService.sync({
        windowHours,
        syncPowerLevels: true,
        syncVotes: true,
      });

      logger.log('✅ [DAILY] Manual indexer sync completed successfully');
      return stats;
    } catch (error) {
      logger.error('❌ [DAILY] Manual indexer sync failed:', error);
      throw error;
    }
  }
}
