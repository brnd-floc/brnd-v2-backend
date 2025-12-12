import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';

import { User, Brand } from '../../../models';
import { getConfig } from '../../../security/config';
import { logger } from '../../../main';
import { RewardService } from './reward.service';

interface CastWebhookPayload {
  data: {
    hash: string;
    author: {
      fid: number;
    };
    text: string;
    timestamp: string;
    parent_hash?: string;
    embeds?: Array<{
      url: string;
    }>;
  };
}

interface ShareDetectionResult {
  isVotingShare: boolean;
  day?: number;
  brandIds?: number[];
  extractedInfo?: any;
}

@Injectable()
export class CastVerificationService {
  private readonly VOTING_KEYWORDS = [
    'voted',
    'podium',
    'brnd',
    'cast your vote',
    'top brands',
  ];
  private readonly SHARE_DETECTION_WINDOW_HOURS = 24;

  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    private readonly rewardService: RewardService,
  ) {}

  async processCastWebhook(payload: CastWebhookPayload): Promise<boolean> {
    try {
      logger.log(
        `📱 [CAST] Processing cast webhook for FID: ${payload.data.author.fid}`,
      );

      const shareDetection = await this.isVotingShare(payload);
      if (!shareDetection.isVotingShare) {
        logger.log(`📱 [CAST] Cast is not a voting share, skipping`);
        return false;
      }

      const fid = payload.data.author.fid;
      const castTimestamp = new Date(payload.data.timestamp);
      const day = Math.floor(castTimestamp.getTime() / (1000 * 60 * 60 * 24));

      // Verify share authenticity
      const isAuthentic = await this.verifyShareAuthenticity(fid, day, payload);
      if (!isAuthentic) {
        logger.log(
          `❌ [CAST] Share authenticity verification failed for FID: ${fid}`,
        );
        return false;
      }

      // Mark share as verified for reward
      const verified = await this.rewardService.verifyShareForReward(
        fid,
        day,
        payload.data.hash,
      );

      if (verified) {
        logger.log(
          `✅ [CAST] Share verified and marked for rewards - FID: ${fid}, Day: ${day}`,
        );
      }

      return verified;
    } catch (error) {
      logger.error('Error processing cast webhook:', error);
      return false;
    }
  }

  async isVotingShare(cast: CastWebhookPayload): Promise<ShareDetectionResult> {
    try {
      const text = cast.data.text.toLowerCase();

      // Check for voting-related keywords
      const hasVotingKeywords = this.VOTING_KEYWORDS.some((keyword) =>
        text.includes(keyword.toLowerCase()),
      );

      if (!hasVotingKeywords) {
        return { isVotingShare: false };
      }

      // Extract potential brand mentions
      const extractedInfo = await this.extractVotingInfo(cast);

      return {
        isVotingShare: true,
        day: extractedInfo.day,
        brandIds: extractedInfo.brandIds,
        extractedInfo,
      };
    } catch (error) {
      logger.error('Error detecting voting share:', error);
      return { isVotingShare: false };
    }
  }

  async extractVotingInfo(cast: CastWebhookPayload): Promise<{
    day: number;
    brandIds?: number[];
    isValidShare: boolean;
  }> {
    try {
      const castTimestamp = new Date(cast.data.timestamp);
      const day = Math.floor(castTimestamp.getTime() / (1000 * 60 * 60 * 24));

      const text = cast.data.text.toLowerCase();

      // Try to extract brand mentions from the cast
      const brands = await this.brandRepository.find();
      const mentionedBrands = brands.filter(
        (brand) =>
          text.includes(brand.name.toLowerCase()) ||
          (brand.onChainHandle &&
            text.includes(brand.onChainHandle.toLowerCase())),
      );

      const brandIds = mentionedBrands.slice(0, 3).map((brand) => brand.id);

      return {
        day,
        brandIds: brandIds.length > 0 ? brandIds : undefined,
        isValidShare: brandIds.length > 0 || this.hasVotingContext(text),
      };
    } catch (error) {
      logger.error('Error extracting voting info:', error);
      return {
        day: Math.floor(Date.now() / (1000 * 60 * 60 * 24)),
        isValidShare: false,
      };
    }
  }

  async verifyShareAuthenticity(
    userFid: number,
    day: number,
    cast: CastWebhookPayload,
  ): Promise<boolean> {
    try {
      // Check if user actually voted on this day
      const user = await this.userRepository.findOne({
        where: { fid: userFid },
      });
      if (!user) {
        logger.log(`❌ [CAST] User not found for FID: ${userFid}`);
        return false;
      }

      // Verify that user voted on this day (or close to it)
      const dayDifference = Math.abs(user.lastVoteDay - day);
      if (dayDifference > 1) {
        logger.log(
          `❌ [CAST] User hasn't voted recently - FID: ${userFid}, Last vote day: ${user.lastVoteDay}, Share day: ${day}`,
        );
        return false;
      }

      // Check timing - cast should be within reasonable time after voting
      const castTime = new Date(cast.data.timestamp);
      const currentTime = new Date();
      const hoursDifference =
        (currentTime.getTime() - castTime.getTime()) / (1000 * 60 * 60);

      if (hoursDifference > this.SHARE_DETECTION_WINDOW_HOURS) {
        logger.log(`❌ [CAST] Cast is too old - ${hoursDifference} hours ago`);
        return false;
      }

      // Additional verification: check if cast contains voting-related content
      const shareDetection = await this.isVotingShare(cast);
      if (!shareDetection.isVotingShare) {
        logger.log(`❌ [CAST] Cast doesn't contain voting content`);
        return false;
      }

      logger.log(`✅ [CAST] Share authenticity verified for FID: ${userFid}`);
      return true;
    } catch (error) {
      logger.error(
        `Error verifying share authenticity for FID ${userFid}:`,
        error,
      );
      return false;
    }
  }

  private hasVotingContext(text: string): boolean {
    const votingPhrases = [
      'my podium',
      'my top 3',
      'voted for',
      'cast my vote',
      'daily vote',
      'brand ranking',
      'today i voted',
    ];

    return votingPhrases.some((phrase) => text.includes(phrase));
  }

  async manuallyVerifyShare(
    castHash: string,
    userFid: number,
    day: number,
  ): Promise<{
    verified: boolean;
    rewardAmount?: string;
    canClaimNow?: boolean;
  }> {
    try {
      logger.log(
        `🔍 [CAST] Manual verification for cast: ${castHash}, FID: ${userFid}, Day: ${day}`,
      );

      // In a real implementation, you would fetch the cast from Farcaster API
      // For now, we'll verify based on user's voting status
      const user = await this.userRepository.findOne({
        where: { fid: userFid },
      });
      if (!user) {
        return { verified: false };
      }

      // Check if user voted on this day
      if (Math.abs(user.lastVoteDay - day) > 1) {
        return { verified: false };
      }

      // Mark as verified
      const verified = await this.rewardService.verifyShareForReward(
        userFid,
        day,
        castHash,
      );

      if (verified) {
        const rewardAmount = this.rewardService.calculateRewardAmount(
          user.brndPowerLevel,
        );
        return {
          verified: true,
          rewardAmount,
          canClaimNow: true,
        };
      }

      return { verified: false };
    } catch (error) {
      logger.error('Error in manual share verification:', error);
      return { verified: false };
    }
  }

  async getShareStatus(
    userFid: number,
    day: number,
  ): Promise<{
    hasShared: boolean;
    sharedAt?: string;
    castHash?: string;
    verified: boolean;
  }> {
    try {
      // This would typically query your reward claims table
      // For now, return basic status
      const eligibility = await this.rewardService.validateRewardEligibility(
        userFid,
        day,
      );

      return {
        hasShared: eligibility.hasShared,
        verified: eligibility.hasShared,
      };
    } catch (error) {
      logger.error(`Error getting share status for FID ${userFid}:`, error);
      return {
        hasShared: false,
        verified: false,
      };
    }
  }
}
