import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';

import { User } from '../../../models';
import { BlockchainService } from './blockchain.service';
import { logger } from '../../../main';
import { getConfig } from '../../../security/config';

export interface PowerLevel {
  id: number;
  title: string;
  description: string;
  multiplier: number;
  leaderboardPoints: number;
  podiumPoints: number;
  shareReward: number;
  isCompleted: boolean;
  isActive: boolean;
  actionType: 'follow' | 'stake' | 'streak' | 'podiums' | 'collectibles';
  actionValue?: string;
  progress?: { current: number; total: number; maxStreak?: number };
  showButton: boolean;
  requirement?: {
    type: string;
    value: number | string;
    unit: string;
  };
}

export interface PowerLevelValidation {
  eligible: boolean;
  reason?: string;
  currentLevel: number;
  nextLevel: number;
  requirements: {
    met: boolean;
    description: string;
    current?: number;
    required?: number;
  }[];
}

@Injectable()
export class PowerLevelService {
  private readonly POWER_LEVELS: PowerLevel[] = [
    {
      id: 1,
      title: 'FOLLOW @BRND',
      description: 'x1 rewards',
      multiplier: 1,
      leaderboardPoints: 10,
      podiumPoints: 100,
      shareReward: 1000,
      isCompleted: false,
      isActive: true,
      actionType: 'follow',
      showButton: true,
      requirement: {
        type: 'follow',
        value: '@brnd',
        unit: 'account',
      },
    },
    {
      id: 2,
      title: 'STAKE 2M $BRND',
      description: 'x2 rewards',
      multiplier: 2,
      leaderboardPoints: 12,
      podiumPoints: 200,
      shareReward: 2000,
      isCompleted: false,
      isActive: false,
      actionType: 'stake',
      actionValue: '2M',
      showButton: true,
      requirement: {
        type: 'stake',
        value: 2_000_000,
        unit: 'BRND',
      },
    },
    {
      id: 3,
      title: 'PODIUM STREAK: 5 DAYS',
      description: 'x3 rewards',
      multiplier: 3,
      leaderboardPoints: 18,
      podiumPoints: 300,
      shareReward: 3000,
      isCompleted: false,
      isActive: false,
      actionType: 'streak',
      progress: { current: 0, total: 5 },
      showButton: false,
      requirement: {
        type: 'streak',
        value: 5,
        unit: 'days',
      },
    },
    {
      id: 4,
      title: 'STAKE 4M $BRND',
      description: 'x4 rewards',
      multiplier: 4,
      leaderboardPoints: 24,
      podiumPoints: 400,
      shareReward: 4000,
      isCompleted: false,
      isActive: false,
      actionType: 'stake',
      actionValue: '4M',
      showButton: true,
      requirement: {
        type: 'stake',
        value: 4_000_000,
        unit: 'BRND',
      },
    },
    {
      id: 5,
      title: 'VOTE 100 PODIUMS',
      description: 'x5 rewards',
      multiplier: 5,
      leaderboardPoints: 30,
      podiumPoints: 500,
      shareReward: 5000,
      isCompleted: false,
      isActive: false,
      actionType: 'podiums',
      progress: { current: 0, total: 100 },
      showButton: false,
      requirement: {
        type: 'podiums',
        value: 100,
        unit: 'votes',
      },
    },
    {
      id: 6,
      title: 'STAKE 6M $BRND',
      description: 'x6 rewards',
      multiplier: 6,
      leaderboardPoints: 36,
      podiumPoints: 600,
      shareReward: 6000,
      isCompleted: false,
      isActive: false,
      actionType: 'stake',
      actionValue: '6M',
      showButton: true,
      requirement: {
        type: 'stake',
        value: 6_000_000,
        unit: 'BRND',
      },
    },
    {
      id: 7,
      title: 'COLLECT 7 BRND COLLECTIBLE CASTS',
      description: 'x7 BRND COLLECTIBLE CASTS rewards',
      multiplier: 7,
      leaderboardPoints: 42,
      podiumPoints: 700,
      shareReward: 7000,
      isCompleted: false,
      isActive: false,
      actionType: 'collectibles',
      actionValue: '7',
      showButton: false,
      requirement: {
        type: 'collectibles',
        value: 7,
        unit: 'casts',
      },
    },
    {
      id: 8,
      title: 'STAKE 8M $BRND',
      description: 'x8 rewards',
      multiplier: 8,
      leaderboardPoints: 48,
      podiumPoints: 800,
      shareReward: 8000,
      isCompleted: false,
      isActive: false,
      actionType: 'stake',
      actionValue: '8M',
      showButton: true,
      requirement: {
        type: 'stake',
        value: 8_000_000,
        unit: 'BRND',
      },
    },
  ];

  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    private readonly blockchainService: BlockchainService,
  ) {}

  /**
   * Gets the user's power level information with completion status and progress.
   *
   * @returns {
   *   currentLevel: number - The user's current level from the smart contract
   *   currentPowerLevel: PowerLevel - The current level object with details
   *   nextLevel?: PowerLevel - The next level to work on (undefined if at max level)
   *     - isCompleted: boolean - Whether requirements are met (can level up)
   *     - progress?: { current: number, total: number } - Progress tracking for stake/streak/podiums/collectibles
   *     - requirement: { type, value, unit } - What needs to be done
   *     - isActive: boolean - Whether this is the active level to work on
   *   allLevels: PowerLevel[] - All levels with their completion status and progress
   *   progress: {
   *     stakedAmount: number - Current staked BRND tokens
   *     totalBalance: number - Total BRND balance (staked + unstaked)
   *     followingBrnd: boolean - Whether following @brnd
   *     dailyStreak: number - Current voting streak
   *     totalPodiums: number - Total podium votes
   *     collectibles: number - Number of collectibles owned
   *   }
   * }
   */
  async getUserPowerLevel(fid: number): Promise<{
    currentLevel: number;
    currentPowerLevel: PowerLevel;
    nextLevel?: PowerLevel;
    allLevels: PowerLevel[];
    progress: any;
  }> {
    try {
      logger.log(`📈 [POWER LEVEL] Getting power level for FID: ${fid}`);

      const user = await this.userRepository.findOne({
        where: { fid },
        relations: ['userBrandVotes'],
      });

      if (!user) {
        throw new Error('User not found');
      }

      // Get user's blockchain data
      const [stakeInfo, followStatus] = await Promise.all([
        this.blockchainService.getUserStakeInfo(fid),
        this.blockchainService.checkFollowStatus(fid),
      ]);

      // Calculate current achievements
      const achievements = await this.calculateAchievements(
        user,
        stakeInfo,
        followStatus,
      );

      // Get actual current level from V5 contract (not from achievements)
      const contractUserInfo =
        await this.blockchainService.getUserInfoFromContractByFid(fid);
      const currentLevel = contractUserInfo
        ? contractUserInfo.brndPowerLevel
        : 0;

      logger.log(
        `📋 [POWER LEVEL] Contract level for FID ${fid}: ${currentLevel}`,
      );

      // Determine completion status based on achievements (for eligibility)
      const levelsWithProgress = this.POWER_LEVELS.map((level) => {
        const levelCopy = { ...level };
        const isCompleted = this.isLevelCompleted(level, achievements);

        levelCopy.isCompleted = isCompleted;

        // Update progress for tracking levels
        if (level.actionType === 'stake') {
          const requiredStake = level.requirement?.value as number;
          levelCopy.progress = {
            current: Math.floor(achievements.stakedAmount),
            total: requiredStake,
          };
        } else if (level.actionType === 'streak') {
          levelCopy.progress = {
            current: achievements.dailyStreak,
            total: level.requirement?.value as number,
            maxStreak: achievements.maxDailyStreak,
          };
        } else if (level.actionType === 'podiums') {
          levelCopy.progress = {
            current: achievements.totalPodiums,
            total: level.requirement?.value as number,
          };
        } else if (level.actionType === 'collectibles') {
          levelCopy.progress = {
            current: achievements.collectibles,
            total: level.requirement?.value as number,
          };
        }

        // Set active status
        levelCopy.isActive = level.id === currentLevel + 1 || level.id === 1;

        return levelCopy;
      });

      const currentPowerLevel = levelsWithProgress.find(
        (l) => l.id === currentLevel,
      );
      const nextLevel = levelsWithProgress.find(
        (l) => l.id === currentLevel + 1,
      );

      return {
        currentLevel,
        currentPowerLevel: currentPowerLevel || levelsWithProgress[0], // Default to level 1 if none completed
        nextLevel,
        allLevels: levelsWithProgress,
        progress: achievements,
      };
    } catch (error) {
      logger.error('Error getting user power level:', error);
      throw error;
    }
  }

  async canLevelUp(
    fid: number,
    targetLevel: number,
  ): Promise<PowerLevelValidation> {
    try {
      logger.log(
        `✅ [POWER LEVEL] Checking if FID ${fid} can level up to ${targetLevel}`,
      );

      const powerLevelData = await this.getUserPowerLevel(fid);

      // Find the target level with calculated completion status
      const targetPowerLevel = powerLevelData.allLevels.find(
        (l) => l.id === targetLevel,
      );

      if (!targetPowerLevel) {
        return {
          eligible: false,
          reason: 'Invalid target level',
          currentLevel: powerLevelData.currentLevel,
          nextLevel: targetLevel,
          requirements: [],
        };
      }

      if (targetLevel <= powerLevelData.currentLevel) {
        return {
          eligible: false,
          reason: 'Target level must be higher than current level',
          currentLevel: powerLevelData.currentLevel,
          nextLevel: targetLevel,
          requirements: [],
        };
      }

      if (targetLevel !== powerLevelData.currentLevel + 1) {
        // If trying to skip levels, check requirements for the next level
        const nextLevel = powerLevelData.currentLevel + 1;
        const nextPowerLevel = powerLevelData.allLevels.find(
          (l) => l.id === nextLevel,
        );

        let nextLevelRequirements = [];
        if (nextPowerLevel) {
          nextLevelRequirements = await this.getLevelRequirements(
            fid,
            nextPowerLevel,
            powerLevelData.progress,
          );
        }

        return {
          eligible: false,
          reason: 'Can only level up one level at a time',
          currentLevel: powerLevelData.currentLevel,
          nextLevel: targetLevel,
          requirements: nextLevelRequirements,
        };
      }

      // Use the calculated completion status from achievements
      const isCompleted = targetPowerLevel.isCompleted;
      const requirements = await this.getLevelRequirements(
        fid,
        targetPowerLevel,
        powerLevelData.progress,
      );

      return {
        eligible: isCompleted,
        reason: isCompleted ? undefined : 'Requirements not met',
        currentLevel: powerLevelData.currentLevel,
        nextLevel: targetLevel,
        requirements,
      };
    } catch (error) {
      logger.error('Error checking level up eligibility:', error);
      throw error;
    }
  }

  private async calculateAchievements(
    user: User,
    stakeInfo: any,
    followStatus: any,
  ): Promise<any> {
    const totalPodiums = user.userBrandVotes?.length || 0;

    // Calculate maxDailyStreak if it's null
    let maxDailyStreak = user.maxDailyStreak;
    if (maxDailyStreak === null) {
      maxDailyStreak = await this.calculateMaxStreakFromHistory(user);
      // Update the user record with the calculated max streak
      await this.userRepository.update(user.id, { maxDailyStreak });
    }

    // Fetch collectibles count from the indexer API
    const collectibles = await this.fetchCollectiblesCount(user.fid);

    return {
      followingBrnd: followStatus.followingBrnd,
      followingFloc: followStatus.followingFloc,
      stakedAmount: Math.floor(stakeInfo.stakedAmount),
      totalBalance: Math.floor(stakeInfo.totalBalance),
      dailyStreak: user.dailyStreak || 0,
      maxDailyStreak: maxDailyStreak || 0,
      totalPodiums: user.totalPodiums || totalPodiums,
      votedBrandsCount: user.votedBrandsCount || 0,
      collectibles,
    };
  }

  private async fetchCollectiblesCount(fid: number): Promise<number> {
    try {
      const config = getConfig();
      const apiRoute = config.collectiblesIndexer?.apiRoute;
      const apiKey = config.collectiblesIndexer?.apiKey;

      if (!apiRoute || !apiKey) {
        logger.warn(
          `⚠️ [POWER LEVEL] Collectibles indexer API route or key not configured`,
        );
        return 0;
      }

      const url = `${apiRoute}/brnd-airdrop/collected-casts?collectorFid=${fid}`;

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'X-API-KEY': apiKey,
        },
      });

      if (!response.ok) {
        logger.error(
          `❌ [POWER LEVEL] Failed to fetch collectibles for FID ${fid}: ${response.status} ${response.statusText}`,
        );
        return 0;
      }

      const data = await response.json();
      const count = data?.count || data?.collectedCasts?.length || 0;

      logger.log(
        `✅ [POWER LEVEL] Fetched ${count} collectibles for FID ${fid}`,
      );

      return count;
    } catch (error) {
      logger.error(
        `❌ [POWER LEVEL] Error fetching collectibles for FID ${fid}:`,
        error,
      );
      return 0;
    }
  }

  private async calculateMaxStreakFromHistory(user: User): Promise<number> {
    try {
      // Get all user votes ordered by date
      const votes = await this.userRepository
        .createQueryBuilder('user')
        .leftJoinAndSelect('user.userBrandVotes', 'votes')
        .where('user.id = :userId', { userId: user.id })
        .getOne();

      if (!votes?.userBrandVotes || votes.userBrandVotes.length === 0) {
        return Math.max(user.dailyStreak || 0, 0);
      }

      // Group votes by day and calculate max streak
      const voteDates = votes.userBrandVotes
        .map((vote) => {
          const date = new Date(vote.date);
          date.setHours(0, 0, 0, 0);
          return date.getTime();
        })
        .sort((a, b) => b - a); // Most recent first

      const uniqueDays = [...new Set(voteDates)].sort((a, b) => a - b); // Oldest first for streak calculation

      if (uniqueDays.length === 0) {
        return Math.max(user.dailyStreak || 0, 0);
      }

      let maxStreak = 0;
      let currentStreak = 1;

      for (let i = 1; i < uniqueDays.length; i++) {
        const currentDay = uniqueDays[i];
        const previousDay = uniqueDays[i - 1];
        const dayDiff = (currentDay - previousDay) / (1000 * 60 * 60 * 24);

        if (dayDiff === 1) {
          // Consecutive day
          currentStreak++;
        } else {
          // Gap in voting, update max and reset current
          maxStreak = Math.max(maxStreak, currentStreak);
          currentStreak = 1;
        }
      }

      // Final check for the last streak
      maxStreak = Math.max(maxStreak, currentStreak);

      // Compare with current daily streak in case it's higher
      return Math.max(maxStreak, user.dailyStreak || 0);
    } catch (error) {
      logger.error('Error calculating max streak from history:', error);
      return Math.max(user.dailyStreak || 0, 0);
    }
  }

  private isLevelCompleted(level: PowerLevel, achievements: any): boolean {
    switch (level.actionType) {
      case 'follow':
        return achievements.followingBrnd;

      case 'stake':
        const requiredStake = level.requirement?.value as number;
        return achievements.stakedAmount >= requiredStake;

      case 'streak':
        const requiredStreak = level.requirement?.value as number;
        return achievements.maxDailyStreak >= requiredStreak;

      case 'podiums':
        const requiredPodiums = level.requirement?.value as number;
        return achievements.totalPodiums >= requiredPodiums;

      case 'collectibles':
        const requiredCollectibles = level.requirement?.value as number;
        return achievements.collectibles >= requiredCollectibles;

      default:
        return false;
    }
  }

  private async getLevelRequirements(
    fid: number,
    level: PowerLevel,
    achievements: any,
  ): Promise<
    Array<{
      met: boolean;
      description: string;
      current?: number;
      required?: number;
    }>
  > {
    const requirements = [];

    switch (level.actionType) {
      case 'follow':
        requirements.push({
          met: achievements.followingBrnd,
          description: 'Follow @brnd on Farcaster',
          current: achievements.followingBrnd ? 1 : 0,
          required: 1,
        });
        break;

      case 'stake':
        const requiredStake = level.requirement?.value as number;
        requirements.push({
          met: achievements.stakedAmount >= requiredStake,
          description: `Stake ${(requiredStake / 1_000_000).toLocaleString()}M BRND tokens`,
          current: Math.round(achievements.stakedAmount),
          required: requiredStake,
        });
        break;

      case 'streak':
        const requiredStreak = level.requirement?.value as number;
        requirements.push({
          met: achievements.maxDailyStreak >= requiredStreak,
          description: `Achieve a ${requiredStreak}-day voting streak`,
          current: achievements.maxDailyStreak,
          required: requiredStreak,
        });
        break;

      case 'podiums':
        const requiredPodiums = level.requirement?.value as number;
        requirements.push({
          met: achievements.totalPodiums >= requiredPodiums,
          description: `Vote in ${requiredPodiums} podiums`,
          current: achievements.totalPodiums,
          required: requiredPodiums,
        });
        break;

      case 'collectibles':
        const requiredCollectibles = level.requirement?.value as number;
        requirements.push({
          met: achievements.collectibles >= requiredCollectibles,
          description: `Collect ${requiredCollectibles} BRND cast collectibles`,
          current: achievements.collectibles,
          required: requiredCollectibles,
        });
        break;
    }

    return requirements;
  }
}
