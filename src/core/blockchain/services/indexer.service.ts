import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between } from 'typeorm';

import { Brand, User, UserBrandVotes } from '../../../models';
import { UserService } from '../../user/services';
import { BrandService } from '../../brand/services';
import { logger } from '../../../main';
import { getConfig } from '../../../security/config';
import {
  SubmitVoteDto,
  SubmitBrandDto,
  SubmitRewardClaimDto,
  UpdateUserLevelDto,
} from '../dto';

@Injectable()
export class IndexerService {
  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
    private readonly userService: UserService,
    private readonly brandService: BrandService,
  ) {}

  /**
   * Checks if a vote date falls within the current week
   * Week ends on Friday at midnight UTC (Saturday 00:00:00 UTC)
   * Week runs from Saturday 00:00:00 UTC to Friday 23:59:59 UTC
   */
  private isWithinCurrentWeek(voteDate: Date): boolean {
    const now = new Date();
    const voteDateUTC = new Date(
      Date.UTC(
        voteDate.getUTCFullYear(),
        voteDate.getUTCMonth(),
        voteDate.getUTCDate(),
        voteDate.getUTCHours(),
        voteDate.getUTCMinutes(),
        voteDate.getUTCSeconds(),
      ),
    );
    const nowUTC = new Date(
      Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        now.getUTCHours(),
        now.getUTCMinutes(),
        now.getUTCSeconds(),
      ),
    );

    // Get the current day of week (0 = Sunday, 5 = Friday, 6 = Saturday)
    const currentDayOfWeek = nowUTC.getUTCDay();

    // Calculate days to subtract to get to the start of current week (Saturday)
    // If today is Saturday (6), subtract 0 days (week started today)
    // If today is Sunday (0), subtract 1 day (week started yesterday)
    // If today is Monday (1), subtract 2 days
    // ... If today is Friday (5), subtract 6 days (week started last Saturday)
    const daysToSubtract =
      currentDayOfWeek === 6 ? 0 : (currentDayOfWeek + 1) % 7;

    // Calculate the start of current week (most recent Saturday at 00:00:00 UTC)
    const weekStart = new Date(nowUTC);
    weekStart.setUTCDate(nowUTC.getUTCDate() - daysToSubtract);
    weekStart.setUTCHours(0, 0, 0, 0);

    // Calculate the end of current week (Friday at 23:59:59.999 UTC)
    // Week ends when it becomes Saturday 00:00:00 UTC, so Friday 23:59:59.999 is the last moment
    const weekEnd = new Date(weekStart);
    weekEnd.setUTCDate(weekStart.getUTCDate() + 6); // Add 6 days to get to Friday
    weekEnd.setUTCHours(23, 59, 59, 999);

    return voteDateUTC >= weekStart && voteDateUTC <= weekEnd;
  }

  /**
   * Checks if a vote date falls within the current day
   * Day runs from 00:00:00 UTC to 23:59:59 UTC
   * Day ends at midnight UTC (next day 00:00:00 UTC)
   */
  private isWithinCurrentDay(voteDate: Date): boolean {
    const now = new Date();
    const voteDateUTC = new Date(
      Date.UTC(
        voteDate.getUTCFullYear(),
        voteDate.getUTCMonth(),
        voteDate.getUTCDate(),
        voteDate.getUTCHours(),
        voteDate.getUTCMinutes(),
        voteDate.getUTCSeconds(),
      ),
    );
    const nowUTC = new Date(
      Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        now.getUTCHours(),
        now.getUTCMinutes(),
        now.getUTCSeconds(),
      ),
    );

    // Check if vote is on the same day (year, month, and date)
    return (
      voteDateUTC.getUTCFullYear() === nowUTC.getUTCFullYear() &&
      voteDateUTC.getUTCMonth() === nowUTC.getUTCMonth() &&
      voteDateUTC.getUTCDate() === nowUTC.getUTCDate()
    );
  }

  /**
   * Checks if a vote date falls within the current month
   * Month ends at the end of the month at midnight UTC
   */
  private isWithinCurrentMonth(voteDate: Date): boolean {
    const now = new Date();
    const voteDateUTC = new Date(
      Date.UTC(
        voteDate.getUTCFullYear(),
        voteDate.getUTCMonth(),
        voteDate.getUTCDate(),
        voteDate.getUTCHours(),
        voteDate.getUTCMinutes(),
        voteDate.getUTCSeconds(),
      ),
    );
    const nowUTC = new Date(
      Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        now.getUTCHours(),
        now.getUTCMinutes(),
        now.getUTCSeconds(),
      ),
    );

    // Check if vote is in the same year and month
    return (
      voteDateUTC.getUTCFullYear() === nowUTC.getUTCFullYear() &&
      voteDateUTC.getUTCMonth() === nowUTC.getUTCMonth()
    );
  }

  /**
   * Handles vote submission from the Ponder indexer
   */
  async handleVoteSubmission(voteData: SubmitVoteDto): Promise<void> {
    logger.log(`🗳️ [INDEXER] Processing vote submission: ${voteData.id}`);

    try {
      // Convert string values to appropriate types
      const dayNumber = parseInt(voteData.day);
      const blockNumber = parseInt(voteData.blockNumber);
      const timestamp = parseInt(voteData.timestamp);
      const voteDate = new Date(timestamp * 1000); // Convert Unix timestamp to Date

      logger.log(`🗳️ [INDEXER] Vote details:`, {
        id: voteData.id,
        voter: voteData.voter,
        fid: voteData.fid,
        day: dayNumber,
        brandIds: voteData.brandIds,
        cost: voteData.cost,
        blockNumber,
        transactionHash: voteData.transactionHash,
        date: voteDate.toISOString(),
      });

      // Find or create user by FID
      let user = await this.userService.getByFid(voteData.fid);
      if (!user) {
        logger.log(
          `👤 [INDEXER] User with FID ${voteData.fid} not found, fetching from Neynar`,
        );

        // Fetch user info from Neynar API
        const neynarUserInfo = await this.getNeynarUserInfo(voteData.fid);

        // Extract user data from Neynar response
        const username = neynarUserInfo?.username;
        const photoUrl = neynarUserInfo?.pfp_url;
        const neynarScore =
          neynarUserInfo?.score ||
          neynarUserInfo?.experimental?.neynar_user_score ||
          0.0;
        const verified =
          neynarUserInfo?.verified_addresses?.eth_addresses?.length > 0 ||
          false;

        // Use verified address from Neynar if available, otherwise use voter address
        const address =
          neynarUserInfo?.verified_addresses?.primary?.eth_address ||
          neynarUserInfo?.verified_addresses?.eth_addresses?.[0] ||
          voteData.voter;

        user = await this.userRepository.save({
          fid: voteData.fid,
          username,
          photoUrl,
          address,
          banned: false,
          powerups: 0,
          points: 0,
          verified,
          neynarScore,
          createdAt: voteDate,
          updatedAt: voteDate,
        });
        logger.log(
          `✅ [INDEXER] Created user from Neynar data: ${user.id} (username: ${username})`,
        );
      }

      // Verify brands exist
      const [brand1, brand2, brand3] = await Promise.all([
        this.brandRepository.findOne({ where: { id: voteData.brandIds[0] } }),
        this.brandRepository.findOne({ where: { id: voteData.brandIds[1] } }),
        this.brandRepository.findOne({ where: { id: voteData.brandIds[2] } }),
      ]);

      if (!brand1 || !brand2 || !brand3) {
        const missingBrands = [];
        if (!brand1) missingBrands.push(voteData.brandIds[0]);
        if (!brand2) missingBrands.push(voteData.brandIds[1]);
        if (!brand3) missingBrands.push(voteData.brandIds[2]);

        logger.error(
          `❌ [INDEXER] Missing brands: ${missingBrands.join(', ')}`,
        );
        throw new Error(`Brands not found: ${missingBrands.join(', ')}`);
      }

      // Check if vote already exists (prevent duplicates)
      // First check by transaction hash for exact duplicate detection
      const existingVoteByTx = await this.userBrandVotesRepository.findOne({
        where: {
          transactionHash: voteData.transactionHash,
        },
      });

      if (existingVoteByTx) {
        logger.log(
          `⚠️ [INDEXER] Vote with transaction hash ${voteData.transactionHash} already exists, skipping`,
        );
        return;
      }

      // First check if this exact transaction was already processed
      const existingVoteByTxHash = await this.userBrandVotesRepository.findOne({
        where: { transactionHash: voteData.transactionHash },
      });

      if (existingVoteByTxHash) {
        logger.log(
          `⚠️ [INDEXER] Transaction ${voteData.transactionHash} already processed, skipping duplicate`,
        );
        return;
      }

      // Then check if user already voted this day (business rule)
      const dayStart = new Date(voteDate);
      dayStart.setHours(0, 0, 0, 0);
      const dayEnd = new Date(voteDate);
      dayEnd.setHours(23, 59, 59, 999);

      const existingVoteByDay = await this.userBrandVotesRepository.findOne({
        where: {
          user: { id: user.id },
          date: Between(dayStart, dayEnd),
        },
        relations: ['user'],
      });

      if (existingVoteByDay) {
        logger.log(
          `⚠️ [INDEXER] User ${user.id} already voted on ${voteDate.toDateString()}, skipping duplicate`,
        );
        return;
      }

      // Calculate day from timestamp (block.timestamp / 86400)
      const day = Math.floor(timestamp / 86400);
      // Calculate brndPaid right away based on user's brndPowerLevel
      let brndPaid: number;
      switch (user.brndPowerLevel) {
        case 0:
          brndPaid = 100;
          break;
        case 1:
          brndPaid = 150;
          break;
        case 2:
          brndPaid = 200;
          break;
        case 3:
          brndPaid = 300;
          break;
        case 4:
          brndPaid = 400;
          break;
        case 5:
          brndPaid = 500;
          break;
        case 6:
          brndPaid = 600;
          break;
        case 7:
          brndPaid = 700;
          break;
        case 8:
          brndPaid = 800;
          break;
      }
      // Calculate reward amount in wei (brndPaid is in BRND units, convert to wei then multiply by 10)
      // 1 BRND = 10^18 wei, so reward = brndPaid * 10^18 * 10 = brndPaid * 10^19
      const WEI_PER_BRND = BigInt(10 ** 18);
      const REWARD_MULTIPLIER = BigInt(10);
      const rewardAmount = (
        BigInt(brndPaid) *
        WEI_PER_BRND *
        REWARD_MULTIPLIER
      ).toString();

      // Create the vote record
      const vote = this.userBrandVotesRepository.create({
        id: voteData.transactionHash, // Use transaction hash as id
        user: { id: user.id },
        brand1: { id: voteData.brandIds[0] },
        brand2: { id: voteData.brandIds[1] },
        brand3: { id: voteData.brandIds[2] },
        date: voteDate,
        shared: false, // Will be updated if user shares their vote
        castHash: null, // Keep null for now, will be populated when user shares
        transactionHash: voteData.transactionHash, // Store blockchain transaction hash
        brndPaidWhenCreatingPodium: brndPaid,
        rewardAmount: rewardAmount, // Store reward amount in wei (cost * 10 in wei)
        day: day, // Store blockchain day
        shareVerified: false, // Will be updated when user shares
      });

      await this.userBrandVotesRepository.save(vote);
      logger.log(`✅ [INDEXER] Saved vote: ${voteData.id}`);

      // Update brand scores (60, 30, 10 points for 1st, 2nd, 3rd place)
      // Always update score, but conditionally update scoreDay, scoreWeek and scoreMonth
      // based on when the vote was actually cast on-chain
      const score1 = 0.6 * vote.brndPaidWhenCreatingPodium;
      const score2 = 0.3 * vote.brndPaidWhenCreatingPodium;
      const score3 = 0.1 * vote.brndPaidWhenCreatingPodium;

      console.log('THE SCORES ARE', score1, score2, score3);

      // Check if vote falls within current day, week and month
      const isInCurrentDay = this.isWithinCurrentDay(voteDate);
      const isInCurrentWeek = this.isWithinCurrentWeek(voteDate);
      const isInCurrentMonth = this.isWithinCurrentMonth(voteDate);

      logger.log(
        `📅 [INDEXER] Vote date: ${voteDate.toISOString()}, In current day: ${isInCurrentDay}, In current week: ${isInCurrentWeek}, In current month: ${isInCurrentMonth}`,
      );

      // Build array of increment operations conditionally
      const incrementOperations = [
        // Always increment score for all brands
        this.brandRepository.increment(
          { id: voteData.brandIds[0] },
          'score',
          score1,
        ),
        this.brandRepository.increment(
          { id: voteData.brandIds[1] },
          'score',
          score2,
        ),
        this.brandRepository.increment(
          { id: voteData.brandIds[2] },
          'score',
          score3,
        ),
      ];

      // Conditionally increment scoreDay if vote is in current day
      if (isInCurrentDay) {
        incrementOperations.push(
          this.brandRepository.increment(
            { id: voteData.brandIds[0] },
            'scoreDay',
            score1,
          ),
          this.brandRepository.increment(
            { id: voteData.brandIds[1] },
            'scoreDay',
            score2,
          ),
          this.brandRepository.increment(
            { id: voteData.brandIds[2] },
            'scoreDay',
            score3,
          ),
        );
      }

      // Conditionally increment scoreWeek if vote is in current week
      if (isInCurrentWeek) {
        incrementOperations.push(
          this.brandRepository.increment(
            { id: voteData.brandIds[0] },
            'scoreWeek',
            score1,
          ),
          this.brandRepository.increment(
            { id: voteData.brandIds[1] },
            'scoreWeek',
            score2,
          ),
          this.brandRepository.increment(
            { id: voteData.brandIds[2] },
            'scoreWeek',
            score3,
          ),
        );
      }

      // Conditionally increment scoreMonth if vote is in current month
      if (isInCurrentMonth) {
        incrementOperations.push(
          this.brandRepository.increment(
            { id: voteData.brandIds[0] },
            'scoreMonth',
            score1,
          ),
          this.brandRepository.increment(
            { id: voteData.brandIds[1] },
            'scoreMonth',
            score2,
          ),
          this.brandRepository.increment(
            { id: voteData.brandIds[2] },
            'scoreMonth',
            score3,
          ),
        );
      }

      await Promise.all(incrementOperations);

      logger.log(`✅ [INDEXER] Updated brand scores for vote: ${voteData.id}`);

      // Update user's last vote timestamp and day FIRST
      await this.userRepository.update(user.id, {
        lastVoteTimestamp: voteDate,
        lastVoteDay: day,
        totalVotes: user.totalVotes + 1,
      });

      // Then update calculated fields (which depend on the updated totalVotes)
      await this.userService.updateUserCalculatedFields(user.id);

      // Calculate leaderboard points based on BRND power level
      // Level 0 -> 3, Level 1 -> 6, Level 2 -> 9, etc.
      // Formula: (level + 1) * 3
      const leaderboardPoints = (user.brndPowerLevel + 1) * 3;
      await this.userService.addPoints(user.id, leaderboardPoints);

      logger.log(`✅ [INDEXER] Vote processing completed: ${voteData.id}`);
    } catch (error) {
      logger.error(`❌ [INDEXER] Error processing vote ${voteData.id}:`, error);
      throw error;
    }
  }

  /**
   * Handles brand creation/update from the Ponder indexer
   */
  async handleBrandSubmission(brandData: SubmitBrandDto): Promise<void> {
    logger.log(`🏷️ [INDEXER] Processing brand submission: ${brandData.id}`);

    try {
      const blockNumber = parseInt(brandData.blockNumber);
      const timestamp = parseInt(brandData.timestamp);
      const createdAtTimestamp = parseInt(brandData.createdAt);
      const createdAtDate = new Date(createdAtTimestamp * 1000);

      logger.log(`🏷️ [INDEXER] Brand details:`, {
        id: brandData.id,
        fid: brandData.fid,
        walletAddress: brandData.walletAddress,
        handle: brandData.handle,
        createdAt: createdAtDate.toISOString(),
        blockNumber,
        transactionHash: brandData.transactionHash,
      });

      // Check if brand already exists
      const existingBrand = await this.brandRepository.findOne({
        where: { id: brandData.id },
      });

      if (existingBrand) {
        logger.log(
          `📝 [INDEXER] Brand ${brandData.id} already exists, updating contract fields`,
        );

        // Update contract-specific fields
        await this.brandRepository.update(
          { id: brandData.id },
          {
            onChainFid: brandData.fid,
            onChainHandle: brandData.handle,
            walletAddress: brandData.walletAddress,
            isUploadedToContract: true,
            updatedAt: new Date(),
          },
        );

        logger.log(`✅ [INDEXER] Updated existing brand: ${brandData.id}`);
      } else {
        logger.log(`🆕 [INDEXER] Creating new brand: ${brandData.id}`);

        // Create new brand from contract data
        // Note: This creates a minimal brand record - admin should populate full details later
        const newBrand = this.brandRepository.create({
          // Don't set id - it's auto-generated
          name: brandData.handle, // Use handle as initial name
          url: `https://warpcast.com/${brandData.handle}`, // Default URL
          warpcastUrl: `https://warpcast.com/${brandData.handle}`, // Default warpcast URL
          description: `Brand created from contract: ${brandData.handle}`,
          imageUrl: '', // Will need to be populated later
          followerCount: 0, // Will be populated when refreshed
          score: 0,
          scoreDay: 0,
          scoreWeek: 0,
          scoreMonth: 0,
          stateScore: 0,
          stateScoreDay: 0,
          stateScoreWeek: 0,
          stateScoreMonth: 0,
          rankingWeek: 0,
          rankingMonth: 0,
          bonusPoints: 0,
          ranking: '0',
          banned: 0,
          currentRanking: 0,
          onChainFid: brandData.fid,
          onChainHandle: brandData.handle,
          walletAddress: brandData.walletAddress,
          totalBrndAwarded: '0',
          availableBrnd: '0',
          onChainCreatedAt: createdAtDate,
          metadataHash: brandData.handle, // Use handle as placeholder
          isUploadedToContract: true,
          createdAt: createdAtDate,
          updatedAt: new Date(),
          // These fields should be populated by admin later:
          category: null, // ManyToOne relation
          profile: '', // Will be populated later
          channel: '', // Will be populated later
          queryType: 1, // Default to profile type
        });

        await this.brandRepository.save(newBrand);
        logger.log(`✅ [INDEXER] Created new brand: ${brandData.id}`);
      }

      logger.log(`✅ [INDEXER] Brand processing completed: ${brandData.id}`);
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Error processing brand ${brandData.id}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Handles reward claim submissions from the Ponder indexer
   */
  async handleRewardClaimSubmission(
    claimData: SubmitRewardClaimDto,
  ): Promise<void> {
    logger.log(
      `💰 [INDEXER] Processing reward claim submission: ${claimData.id}`,
    );

    try {
      // Convert string values to appropriate types
      const dayNumber = parseInt(claimData.day);
      const blockNumber = parseInt(claimData.blockNumber);
      const timestamp = parseInt(claimData.timestamp);
      const claimDate = new Date(timestamp * 1000); // Convert Unix timestamp to Date

      logger.log(`💰 [INDEXER] Reward claim details:`, {
        id: claimData.id,
        recipient: claimData.recipient,
        fid: claimData.fid,
        amount: claimData.amount,
        day: dayNumber,
        castHash: claimData.castHash,
        caller: claimData.caller,
        blockNumber,
        transactionHash: claimData.transactionHash,
        date: claimDate.toISOString(),
      });

      // Check if claim already exists (prevent duplicates by checking claimTxHash)
      const existingClaim = await this.userBrandVotesRepository.findOne({
        where: {
          claimTxHash: claimData.transactionHash,
        },
      });

      if (existingClaim) {
        logger.log(
          `⚠️ [INDEXER] Claim with transaction hash ${claimData.transactionHash} already exists, skipping`,
        );
        return;
      }

      // Find the corresponding UserBrandVotes record by user FID and day
      const userVote = await this.userBrandVotesRepository.findOne({
        where: {
          user: { fid: claimData.fid },
          day: dayNumber,
        },
        relations: ['user'],
      });

      if (userVote) {
        // Update existing vote record with claim data
        logger.log(
          `📝 [INDEXER] Updating UserBrandVotes with reward claim data for FID ${claimData.fid}, day ${dayNumber}`,
        );

        await this.userBrandVotesRepository.update(
          { transactionHash: userVote.transactionHash },
          {
            claimedAt: claimDate,
            claimTxHash: claimData.transactionHash,
            castHash: claimData.castHash,
            shared: true,
            shareVerified: true,
            shareVerifiedAt: claimDate,
          },
        );

        logger.log(
          `✅ [INDEXER] Updated UserBrandVotes: ${userVote.transactionHash}`,
        );
      } else {
        // This shouldn't normally happen - claim should come after vote
        // But create a placeholder record just in case
        logger.warn(
          `⚠️ [INDEXER] No existing vote found for FID ${claimData.fid}, day ${dayNumber}. Creating placeholder.`,
        );

        // Find or create user by FID
        let user = await this.userService.getByFid(claimData.fid);
        if (!user) {
          logger.log(
            `👤 [INDEXER] User with FID ${claimData.fid} not found, fetching from Neynar`,
          );

          // Fetch user info from Neynar API
          const neynarUserInfo = await this.getNeynarUserInfo(claimData.fid);

          // Extract user data from Neynar response
          const username = neynarUserInfo?.username || `user_${claimData.fid}`;
          const photoUrl = neynarUserInfo?.pfp_url || '';
          const neynarScore =
            neynarUserInfo?.score ||
            neynarUserInfo?.experimental?.neynar_user_score ||
            0.0;
          const verified =
            neynarUserInfo?.verified_addresses?.eth_addresses?.length > 0 ||
            false;

          // Use verified address from Neynar if available, otherwise use recipient address
          const address =
            neynarUserInfo?.verified_addresses?.primary?.eth_address ||
            neynarUserInfo?.verified_addresses?.eth_addresses?.[0] ||
            claimData.recipient;

          user = await this.userRepository.save({
            fid: claimData.fid,
            username,
            photoUrl,
            address,
            banned: false,
            powerups: 0,
            points: 0,
            verified,
            neynarScore,
            createdAt: claimDate,
            updatedAt: claimDate,
          });
          logger.log(
            `✅ [INDEXER] Created user from Neynar data: ${user.id} (username: ${username})`,
          );
        }

        // Create placeholder vote record with claim data
        // Use claim transaction hash as primary key since vote transaction doesn't exist
        // This is an edge case where claim came before vote (shouldn't normally happen)
        const placeholderVote = this.userBrandVotesRepository.create({
          id: claimData.transactionHash, // Use transaction hash as id
          transactionHash: claimData.transactionHash, // Use claim tx hash as primary key
          user: { id: user.id },
          // These will be null since we don't have vote data
          brand1: null,
          brand2: null,
          brand3: null,
          date: claimDate,
          day: dayNumber,
          rewardAmount: claimData.amount,
          shared: true,
          shareVerified: true,
          shareVerifiedAt: claimDate,
          castHash: claimData.castHash,
          claimedAt: claimDate,
          claimTxHash: claimData.transactionHash,
        });

        await this.userBrandVotesRepository.save(placeholderVote);
        logger.log(
          `✅ [INDEXER] Created placeholder vote record: ${placeholderVote.transactionHash}`,
        );
      }

      logger.log(
        `✅ [INDEXER] Reward claim processing completed: ${claimData.id}`,
      );
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Error processing reward claim ${claimData.id}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Handles user level update submissions from the Ponder indexer
   */
  async handleUserLevelUpdate(levelUpData: UpdateUserLevelDto): Promise<void> {
    logger.log(
      `📈 [INDEXER] Processing user level update: ${levelUpData.levelUpId}`,
    );

    try {
      const timestamp = parseInt(levelUpData.timestamp);
      const levelUpDate = new Date(timestamp * 1000);

      logger.log(`📈 [INDEXER] Level-up details:`, {
        fid: levelUpData.fid,
        brndPowerLevel: levelUpData.brndPowerLevel,
        wallet: levelUpData.wallet,
        transactionHash: levelUpData.transactionHash,
      });

      // Find or create user by FID
      let user = await this.userService.getByFid(levelUpData.fid);
      if (!user) {
        logger.log(
          `👤 [INDEXER] User with FID ${levelUpData.fid} not found, fetching from Neynar`,
        );

        // Fetch user info from Neynar API
        const neynarUserInfo = await this.getNeynarUserInfo(levelUpData.fid);

        // Extract user data from Neynar response
        const username = neynarUserInfo?.username || `user_${levelUpData.fid}`;
        const photoUrl = neynarUserInfo?.pfp_url || '';
        const neynarScore =
          neynarUserInfo?.score ||
          neynarUserInfo?.experimental?.neynar_user_score ||
          0.0;
        const verified =
          neynarUserInfo?.verified_addresses?.eth_addresses?.length > 0 ||
          false;

        // Use verified address from Neynar if available, otherwise use wallet address
        const address =
          neynarUserInfo?.verified_addresses?.primary?.eth_address ||
          neynarUserInfo?.verified_addresses?.eth_addresses?.[0] ||
          levelUpData.wallet;

        user = await this.userRepository.save({
          fid: levelUpData.fid,
          username,
          photoUrl,
          address,
          banned: false,
          powerups: 0,
          points: 0,
          verified,
          brndPowerLevel: levelUpData.brndPowerLevel,
          neynarScore,
          createdAt: levelUpDate,
          updatedAt: levelUpDate,
        });
        logger.log(
          `✅ [INDEXER] Created user from Neynar data: ${user.id} (username: ${username})`,
        );
      } else {
        // Update existing user's power level and wallet address
        logger.log(
          `📝 [INDEXER] Updating user ${user.id} power level from ${user.brndPowerLevel} to ${levelUpData.brndPowerLevel}`,
        );

        await this.userRepository.update(
          { id: user.id },
          {
            brndPowerLevel: levelUpData.brndPowerLevel,
            address: levelUpData.wallet,
            updatedAt: levelUpDate,
          },
        );
      }

      logger.log(
        `✅ [INDEXER] User level update completed: ${levelUpData.levelUpId}`,
      );
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Error processing user level update ${levelUpData.levelUpId}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Fetches user info from Neynar API
   */
  private async getNeynarUserInfo(fid: number): Promise<any> {
    try {
      logger.log(`🔍 [INDEXER] Fetching user info from Neynar for FID: ${fid}`);
      const apiKey = getConfig().neynar.apiKey.replace(/&$/, '');

      const response = await fetch(
        `https://api.neynar.com/v2/farcaster/user/bulk?fids=${fid}`,
        {
          headers: {
            accept: 'application/json',
            api_key: apiKey,
          },
        },
      );

      if (!response.ok) {
        throw new Error(
          `Neynar API error: ${response.status} ${response.statusText}`,
        );
      }

      const data = await response.json();
      const userInfo = data?.users?.[0] || null;

      if (userInfo) {
        logger.log(
          `✅ [INDEXER] Successfully fetched Neynar user info for FID: ${fid}`,
        );
      } else {
        logger.warn(
          `⚠️ [INDEXER] No user info found in Neynar response for FID: ${fid}`,
        );
      }

      return userInfo;
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Error fetching Neynar user info for FID ${fid}:`,
        error,
      );
      return null;
    }
  }
}
