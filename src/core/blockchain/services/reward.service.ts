import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';

import { UserBrandVotes, User } from '../../../models';
import { getConfig } from '../../../security/config';
import { logger } from '../../../main';
import { SignatureService } from './signature.service';

import { verifyTypedData } from 'viem';

interface RewardEligibility {
  eligible: boolean;
  reason?: string;
  hasVoted: boolean;
  hasShared: boolean;
  hasClaimed: boolean;
  expectedAmount?: string;
}

export interface ClaimStatusResponse {
  canClaim: boolean;
  hasClaimed: boolean;
  shareVerified: boolean;
  amount: string;
  day: number;
  estimatedGas?: string;
}

export interface ClaimRewardResponse {
  signature: string;
  amount: string;
  deadline: number;
  nonce: number;
  canClaim: boolean;
}

@Injectable()
export class RewardService {
  private readonly BASE_VOTE_COST = '100000000000000000000'; // 100 BRND in wei
  private readonly REWARD_MULTIPLIER = 10;
  private readonly WEI_PER_BRND = BigInt(10 ** 18);
  private readonly REWARD_MULTIPLIER_BIGINT = BigInt(10);
  private readonly MIN_WEI_AMOUNT = BigInt(10 ** 18); // 1 BRND in wei

  constructor(
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    private readonly signatureService: SignatureService,
  ) {}

  /**
   * Extracts and validates reward amount from database value.
   * If the stored value is too small (not in wei), recalculates from brndPaid.
   */
  private extractRewardAmount(
    rewardAmount: string | null | undefined,
    brndPaid?: number | null,
  ): string {
    if (!rewardAmount) {
      return '0';
    }

    // Extract integer part if it has decimal places
    const amount = rewardAmount.includes('.')
      ? rewardAmount.split('.')[0]
      : rewardAmount;

    // Validate that the amount is in wei format (should be a very large number)
    // If it's too small (like "1000"), it means it wasn't converted to wei
    const amountBigInt = BigInt(amount);
    if (amountBigInt < this.MIN_WEI_AMOUNT && brndPaid) {
      // Recalculate if the stored value is too small
      const recalculated = (
        BigInt(brndPaid) *
        this.WEI_PER_BRND *
        this.REWARD_MULTIPLIER_BIGINT
      ).toString();
      logger.log(
        `💰 [REWARD] Recalculated amount: ${recalculated} (from brndPaid: ${brndPaid}, original: ${amount})`,
      );
      return recalculated;
    }

    return amount;
  }

  async getClaimStatus(fid: number, day: number): Promise<ClaimStatusResponse> {
    try {
      logger.log(
        `🔍 [REWARD] Checking claim status for FID: ${fid}, Day: ${day}`,
      );

      const user = await this.userRepository.findOne({ where: { fid } });
      if (!user) {
        throw new Error('User not found');
      }

      const vote = await this.userBrandVotesRepository.findOne({
        where: { user: { fid }, day },
        relations: ['user'],
      });

      const hasClaimed = vote?.claimedAt != null;
      const shareVerified = vote?.shareVerified || false;
      const canClaim = shareVerified && !hasClaimed;

      // Extract and validate reward amount
      const amount = this.extractRewardAmount(
        vote?.rewardAmount,
        vote?.brndPaidWhenCreatingPodium,
      );

      return {
        canClaim,
        hasClaimed,
        shareVerified,
        amount,
        day,
      };
    } catch (error) {
      logger.error(`Error checking claim status for FID ${fid}:`, error);
      throw error;
    }
  }

  async generateClaimSignature(
    fid: number,
    day: number,
    recipientAddress: string,
    castHash?: string,
  ): Promise<ClaimRewardResponse> {
    try {
      logger.log(
        `💰 [REWARD] Generating claim signature for FID: ${fid}, Day: ${day}`,
      );

      console.log('Validating reward eligibility');
      const eligibility = await this.validateRewardEligibility(fid, day);
      if (!eligibility.eligible) {
        throw new Error(`Cannot claim reward: ${eligibility.reason}`);
      }
      console.log('Reward eligibility validated');
      // Find the vote for this day to get the reward amount
      const vote = await this.userBrandVotesRepository.findOne({
        where: { user: { fid }, day },
        relations: ['user'],
      });

      if (!vote) {
        throw new Error(`Vote not found for FID ${fid} on day ${day}`);
      }

      if (!vote.rewardAmount) {
        throw new Error(`Reward amount not found for vote on day ${day}`);
      }

      // Extract and validate reward amount (handles wei conversion if needed)
      const amount = this.extractRewardAmount(
        vote.rewardAmount,
        vote.brndPaidWhenCreatingPodium,
      );

      // Log for debugging
      logger.log(
        `💰 [REWARD] Reward amount - Raw: ${vote.rewardAmount}, Extracted: ${amount}, brndPaid: ${vote.brndPaidWhenCreatingPodium}`,
      );

      const deadline = Math.floor(Date.now() / 1000) + 3600; // 1 hour deadline

      const { signature, nonce } =
        await this.signatureService.generateRewardClaimSignature(
          recipientAddress,
          fid,
          amount,
          day,
          castHash || '',
          deadline,
        );

      // Update the vote with signature info
      vote.signatureGeneratedAt = new Date();
      vote.nonce = nonce;
      await this.userBrandVotesRepository.save(vote);

      return {
        signature,
        amount,
        deadline,
        nonce,
        canClaim: true,
      };
    } catch (error) {
      logger.error(`Error generating claim signature for FID ${fid}:`, error);
      throw error;
    }
  }

  async validateRewardEligibility(
    fid: number,
    day: number,
  ): Promise<RewardEligibility> {
    try {
      const user = await this.userRepository.findOne({ where: { fid } });
      if (!user) {
        return {
          eligible: false,
          reason: 'User not found',
          hasVoted: false,
          hasShared: false,
          hasClaimed: false,
        };
      }

      const vote = await this.userBrandVotesRepository.findOne({
        where: { user: { fid }, day },
        relations: ['user'],
      });

      if (!vote) {
        return {
          eligible: false,
          reason: 'User has not voted for this day',
          hasVoted: false,
          hasShared: false,
          hasClaimed: false,
        };
      }

      const hasVoted = true; // Vote exists
      const hasShared = vote.shareVerified || false;
      const hasClaimed = vote.claimedAt != null;

      if (hasClaimed) {
        return {
          eligible: false,
          reason: 'Already claimed for this day',
          hasVoted,
          hasShared,
          hasClaimed: true,
        };
      }

      if (!hasShared) {
        return {
          eligible: false,
          reason: 'User has not shared their cast',
          hasVoted: true,
          hasShared: false,
          hasClaimed: false,
        };
      }

      // Remove decimal part from rewardAmount (database returns decimal format)
      const expectedAmount = vote.rewardAmount
        ? vote.rewardAmount.split('.')[0]
        : '0';

      return {
        eligible: true,
        hasVoted: true,
        hasShared: true,
        hasClaimed: false,
        expectedAmount,
      };
    } catch (error) {
      logger.error(
        `Error validating reward eligibility for FID ${fid}:`,
        error,
      );
      return {
        eligible: false,
        reason: 'Internal error',
        hasVoted: false,
        hasShared: false,
        hasClaimed: false,
      };
    }
  }

  calculateRewardAmount(brndPowerLevel: number): string {
    const voteCost = this.getVoteCost(brndPowerLevel);
    const rewardAmount = BigInt(voteCost) * BigInt(this.REWARD_MULTIPLIER);
    return rewardAmount.toString();
  }

  private getVoteCost(brndPowerLevel: number): string {
    if (brndPowerLevel === 0) return this.BASE_VOTE_COST;
    if (brndPowerLevel >= 8)
      return (BigInt(this.BASE_VOTE_COST) * BigInt(8)).toString();
    return (BigInt(this.BASE_VOTE_COST) * BigInt(brndPowerLevel)).toString();
  }

  async markRewardClaimed(
    fid: number,
    day: number,
    txHash: string,
  ): Promise<void> {
    try {
      const vote = await this.userBrandVotesRepository.findOne({
        where: { user: { fid }, day },
        relations: ['user'],
      });

      if (vote) {
        vote.claimedAt = new Date();
        vote.claimTxHash = txHash;
        await this.userBrandVotesRepository.save(vote);
        logger.log(
          `✅ [REWARD] Marked reward as claimed for FID: ${fid}, Day: ${day}`,
        );
      }
    } catch (error) {
      logger.error(`Error marking reward as claimed for FID ${fid}:`, error);
    }
  }

  async verifyShareForReward(
    fid: number,
    day: number,
    castHash?: string,
  ): Promise<boolean> {
    try {
      logger.log(`🔍 [REWARD] Verifying share for FID: ${fid}, Day: ${day}`);

      const vote = await this.userBrandVotesRepository.findOne({
        where: { user: { fid }, day },
        relations: ['user'],
      });

      if (!vote) {
        logger.log(`❌ [REWARD] Vote not found for FID: ${fid}, Day: ${day}`);
        return false; // Vote doesn't exist
      }

      vote.shareVerified = true;
      vote.shareVerifiedAt = new Date();
      if (castHash) {
        vote.castHash = castHash;
      }

      await this.userBrandVotesRepository.save(vote);
      logger.log(`✅ [REWARD] Share verified for FID: ${fid}, Day: ${day}`);
      return true;
    } catch (error) {
      logger.error(`Error verifying share for FID ${fid}:`, error);
      return false;
    }
  }

  async getUserRewardHistory(fid: number): Promise<{
    totalClaimed: string;
    pendingRewards: Array<{
      day: number;
      amount: string;
      canClaim: boolean;
      shareVerified: boolean;
    }>;
    rewardHistory: Array<{
      day: number;
      amount: string;
      claimedAt: string;
      txHash: string;
    }>;
  }> {
    try {
      const votes = await this.userBrandVotesRepository.find({
        where: { user: { fid } },
        relations: ['user'],
        order: { day: 'DESC' },
      });

      // Filter votes that have reward amounts (votes from indexer)
      const votesWithRewards = votes.filter(
        (vote) => vote.rewardAmount && vote.day != null,
      );

      const claimedRewards = votesWithRewards.filter((vote) => vote.claimedAt);
      const pendingRewards = votesWithRewards.filter((vote) => !vote.claimedAt);

      const totalClaimed = claimedRewards
        .reduce((sum, vote) => {
          const amount = this.extractRewardAmount(
            vote.rewardAmount,
            vote.brndPaidWhenCreatingPodium,
          );
          return sum + BigInt(amount);
        }, BigInt(0))
        .toString();

      const rewardHistory = claimedRewards.map((vote) => ({
        day: vote.day!,
        amount: this.extractRewardAmount(
          vote.rewardAmount,
          vote.brndPaidWhenCreatingPodium,
        ),
        claimedAt: vote.claimedAt!.toISOString(),
        txHash: vote.claimTxHash || '',
      }));

      const pendingRewardsFormatted = pendingRewards.map((vote) => ({
        day: vote.day!,
        amount: this.extractRewardAmount(
          vote.rewardAmount,
          vote.brndPaidWhenCreatingPodium,
        ),
        canClaim: vote.shareVerified || false,
        shareVerified: vote.shareVerified || false,
      }));

      return {
        totalClaimed,
        pendingRewards: pendingRewardsFormatted,
        rewardHistory,
      };
    } catch (error) {
      logger.error(`Error getting reward history for FID ${fid}:`, error);
      throw error;
    }
  }
}
