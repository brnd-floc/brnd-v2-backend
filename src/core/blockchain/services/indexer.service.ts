import { Injectable, Inject, forwardRef } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between, Not, IsNull } from 'typeorm';

import { Brand, User, UserBrandVotes } from '../../../models';
import { UserService } from '../../user/services';
import { BrandService } from '../../brand/services';
import { BlockchainService } from './blockchain.service';
import { logger } from '../../../main';
import { getConfig } from '../../../security/config';
import {
  SubmitVoteDto,
  SubmitBrandDto,
  SubmitRewardClaimDto,
  UpdateUserLevelDto,
} from '../dto';
import { PodiumService } from 'src/core/embeds/services/podium.service';

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
    private readonly podiumService: PodiumService,
    @Inject(forwardRef(() => BlockchainService))
    private readonly blockchainService: BlockchainService,
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

      // Find or create user by FID
      let user = await this.userService.getByFid(voteData.fid);
      if (!user) {
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

      // Calculate day from timestamp (block.timestamp / 86400)
      const day = Math.floor(timestamp / 86400);

      // Check if there's a placeholder vote created from a claim that came before this vote
      // Placeholder votes have null brands and a claimTxHash
      const placeholderVote = await this.userBrandVotesRepository.findOne({
        where: {
          user: { id: user.id },
          day: day,
          brand1: null, // Placeholder indicator
          claimTxHash: Not(IsNull()), // Has claim data
        },
        relations: ['user'],
      });

      let placeholderClaimData: {
        claimedAt?: Date;
        claimTxHash?: string;
        castHash?: string;
        shared?: boolean;
        shareVerified?: boolean;
        shareVerifiedAt?: Date;
      } | null = null;

      if (placeholderVote) {
        logger.log(
          `🔄 [INDEXER] Found placeholder vote from claim event, will merge with actual vote: ${placeholderVote.transactionHash}`,
        );
        // Save claim data from placeholder before deleting it
        placeholderClaimData = {
          claimedAt: placeholderVote.claimedAt,
          claimTxHash: placeholderVote.claimTxHash,
          castHash: placeholderVote.castHash,
          shared: placeholderVote.shared,
          shareVerified: placeholderVote.shareVerified,
          shareVerifiedAt: placeholderVote.shareVerifiedAt,
        };
        // Delete the placeholder since we'll create the real vote with the correct transactionHash
        await this.userBrandVotesRepository.delete({
          transactionHash: placeholderVote.transactionHash,
        });
        logger.log(
          `🗑️ [INDEXER] Deleted placeholder vote: ${placeholderVote.transactionHash}`,
        );
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

      // Use actual on-chain cost from the vote event
      // voteData.cost is in wei (e.g., "100000000000000000000" for 100 BRND)
      // Convert to BRND units by dividing by 10^18
      const WEI_PER_BRND = BigInt(10 ** 18);
      const costInWei = BigInt(voteData.cost);
      const brndPaid = Number(costInWei / WEI_PER_BRND);
      // Calculate reward amount in wei (brndPaid is in BRND units, convert to wei then multiply by 10)
      // 1 BRND = 10^18 wei, so reward = brndPaid * 10^18 * 10 = brndPaid * 10^19
      const REWARD_MULTIPLIER = BigInt(10);
      const rewardAmount = (
        BigInt(brndPaid) *
        WEI_PER_BRND *
        REWARD_MULTIPLIER
      ).toString();

      // Create the vote record
      // If we found a placeholder, merge the claim data into the vote
      // Initial points for voting is 3; will be updated when claimed
      const vote = this.userBrandVotesRepository.create({
        id: voteData.transactionHash, // Use transaction hash as id
        user: { id: user.id },
        brand1: { id: voteData.brandIds[0] },
        brand2: { id: voteData.brandIds[1] },
        brand3: { id: voteData.brandIds[2] },
        date: voteDate,
        shared: placeholderClaimData?.shared || false, // Use placeholder data if available
        castHash: placeholderClaimData?.castHash || null, // Use placeholder castHash if available
        transactionHash: voteData.transactionHash, // Store blockchain transaction hash
        brndPaidWhenCreatingPodium: brndPaid,
        rewardAmount: rewardAmount, // Store reward amount in wei (cost * 10 in wei)
        day: day, // Store blockchain day
        shareVerified: placeholderClaimData?.shareVerified || false, // Use placeholder data if available
        shareVerifiedAt: placeholderClaimData?.shareVerifiedAt || null, // Use placeholder data if available
        claimedAt: placeholderClaimData?.claimedAt || null, // Use placeholder data if available
        claimTxHash: placeholderClaimData?.claimTxHash || null, // Use placeholder data if available
        isLastVoteForCombination: true, // This is now the latest vote for this combination
        pointsEarned: 3, // Initial 3 points for voting
      });

      await this.userBrandVotesRepository.save(vote);

      // Update any previous votes with the same brand combination to no longer be the last
      // This enables frontend to know if a user can mint (only last voter can mint)
      await this.userBrandVotesRepository
        .createQueryBuilder()
        .update(UserBrandVotes)
        .set({ isLastVoteForCombination: false })
        .where('brand1Id = :b1 AND brand2Id = :b2 AND brand3Id = :b3', {
          b1: voteData.brandIds[0],
          b2: voteData.brandIds[1],
          b3: voteData.brandIds[2],
        })
        .andWhere('transactionHash != :txHash', {
          txHash: voteData.transactionHash,
        })
        .execute();

      logger.log(
        `✅ [INDEXER] Updated isLastVoteForCombination flags for brand combination [${voteData.brandIds.join(', ')}]`,
      );

      if (placeholderClaimData) {
        logger.log(
          `✅ [INDEXER] Saved vote and merged placeholder claim data: ${voteData.id}`,
        );
        // Add level-based leaderboard points for the claim (since we merged claim data)
        // User gets brndPowerLevel * 3 additional points
        // Read brndPowerLevel from contract (source of truth)

        let contractLevel = user.brndPowerLevel;
        try {
          const contractInfo =
            await this.blockchainService.getUserInfoFromContractByFid(user.fid);
          contractLevel = contractInfo?.brndPowerLevel ?? user.brndPowerLevel;
        } catch (err) {
          logger.warn(
            `⚠️ [INDEXER] Could not read contract level for FID ${user.fid}, using DB level ${user.brndPowerLevel}`,
          );
        }

        const claimLeaderboardPoints = contractLevel * 3;
        await this.userService.addPoints(user.id, claimLeaderboardPoints);
        logger.log(
          `✅ [INDEXER] Added ${claimLeaderboardPoints} claim points to user ${user.id} (contract level ${contractLevel})`,
        );

        // Update the vote's pointsEarned to include claim points
        const totalPointsEarned = 3 + claimLeaderboardPoints;
        await this.userBrandVotesRepository.update(
          { transactionHash: voteData.transactionHash },
          { pointsEarned: totalPointsEarned },
        );
        logger.log(
          `✅ [INDEXER] Updated vote pointsEarned to ${totalPointsEarned} for tx ${voteData.transactionHash}`,
        );
      } else {
        logger.log(`✅ [INDEXER] Saved vote: ${voteData.id}`);
      }

      // Update brand scores (60, 30, 10 points for 1st, 2nd, 3rd place)
      // Always update score, but conditionally update scoreDay, scoreWeek and scoreMonth
      // based on when the vote was actually cast on-chain
      const score1 = 0.6 * vote.brndPaidWhenCreatingPodium;
      const score2 = 0.3 * vote.brndPaidWhenCreatingPodium;
      const score3 = 0.1 * vote.brndPaidWhenCreatingPodium;

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

      // Optional: Queue ranking update for affected brands (lightweight)
      // Uncomment if you want near real-time ranking updates
      // this.queueRankingUpdate([voteData.brandIds[0], voteData.brandIds[1], voteData.brandIds[2]]);

      // Update user's last vote timestamp and day FIRST
      await this.userRepository.update(user.id, {
        lastVoteTimestamp: voteDate,
        lastVoteDay: day,
        totalVotes: user.totalVotes + 1,
      });

      // Then update calculated fields (which depend on the updated totalVotes)
      await this.userService.updateUserCalculatedFields(user.id);

      // Calculate leaderboard points for voting (flat 3 points regardless of level)
      // Level-based points are awarded separately when reward is claimed
      const leaderboardPoints = 3;
      await this.userService.addPoints(user.id, leaderboardPoints);
      // let cloudinaryUrl: string | null = null;
      // try {
      //   cloudinaryUrl = await this.podiumService.generatePodiumImageFromTxHash(
      //     voteData.transactionHash,
      //   );
      // } catch (error) {
      //   logger.error(`❌ [INDEXER] Error generating podium image:`, error);
      // }

      logger.log(`✅ [INDEXER] Vote processing completed: ${voteData.id}`);
    } catch (error) {
      logger.error(`❌ [INDEXER] Error processing vote ${voteData.id}:`, error);
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

      // Find the corresponding UserBrandVotes record
      // First try by castHash (most reliable)
      let userVote = await this.userBrandVotesRepository.findOne({
        where: {
          castHash: claimData.castHash,
        },
        relations: ['user'],
      });

      // If not found by castHash, try by user FID and day (in case castHash isn't set yet)
      if (!userVote) {
        userVote = await this.userBrandVotesRepository.findOne({
          where: {
            user: { fid: claimData.fid },
            day: dayNumber,
          },
          relations: ['user'],
        });
      }

      if (userVote) {
        // Update existing vote record with claim data
        logger.log(
          `📝 [INDEXER] Updating UserBrandVotes with reward claim data for FID ${claimData.fid}, day ${dayNumber}`,
        );

        // Add level-based leaderboard points when reward is claimed
        // User gets brndPowerLevel * 3 additional points
        // Read brndPowerLevel from contract (source of truth)
        let contractLevel = userVote.user.brndPowerLevel;
        try {
          const contractInfo =
            await this.blockchainService.getUserInfoFromContractByFid(
              userVote.user.fid,
            );
          contractLevel =
            contractInfo?.brndPowerLevel ?? userVote.user.brndPowerLevel;
        } catch (err) {
          logger.warn(
            `⚠️ [INDEXER] Could not read contract level for FID ${userVote.user.fid}, using DB level ${userVote.user.brndPowerLevel}`,
          );
        }

        const claimLeaderboardPoints = contractLevel * 3;

        // Calculate total points earned: 3 (voting) + claim points
        const totalPointsEarned = 3 + claimLeaderboardPoints;

        await this.userBrandVotesRepository.update(
          { transactionHash: userVote.transactionHash },
          {
            claimedAt: claimDate,
            claimTxHash: claimData.transactionHash,
            castHash: claimData.castHash,
            shared: true,
            shareVerified: true,
            shareVerifiedAt: claimDate,
            pointsEarned: totalPointsEarned,
          },
        );

        logger.log(
          `✅ [INDEXER] Updated UserBrandVotes: ${userVote.transactionHash} with pointsEarned: ${totalPointsEarned}`,
        );

        await this.userService.addPoints(
          userVote.user.id,
          claimLeaderboardPoints,
        );
        logger.log(
          `✅ [INDEXER] Added ${claimLeaderboardPoints} claim points to user ${userVote.user.id} (contract level ${contractLevel})`,
        );
      } else {
        // Edge case: Claim event came before vote event
        // Create a placeholder vote record that will be updated when the vote event arrives
        logger.warn(
          `⚠️ [INDEXER] No existing vote found for FID ${claimData.fid}, castHash ${claimData.castHash}. Creating placeholder vote record.`,
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
        // Use claim transaction hash as primary key since vote transaction doesn't exist yet
        // This is an edge case where claim came before vote (shouldn't normally happen)
        const placeholderVote = this.userBrandVotesRepository.create({
          transactionHash: claimData.transactionHash, // Use claim tx hash as primary key
          user: { id: user.id },
          // These will be null since we don't have vote data yet
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
        if (user.brndPowerLevel < levelUpData.brndPowerLevel) {
          await this.userRepository.update(
            { id: user.id },
            {
              brndPowerLevel: levelUpData.brndPowerLevel,
              address: levelUpData.wallet,
              updatedAt: levelUpDate,
            },
          );
          logger.log(
            `✅ [INDEXER] User level update completed: ${levelUpData.levelUpId}`,
          );
        }
      }
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
