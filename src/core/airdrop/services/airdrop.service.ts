import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Not, In } from 'typeorm';
import {
  AirdropScore,
  AirdropSnapshot,
  AirdropLeaf,
  User,
} from '../../../models';
import { MerkleTree } from 'merkletreejs';
import { getConfig } from '../../../security/config';
import { StandardMerkleTree } from '@openzeppelin/merkle-tree';
import { keccak256, AbiCoder } from 'ethers';
import { StandardMerkleTreeData } from '@openzeppelin/merkle-tree/dist/standard';

export interface AirdropMultipliers {
  followAccounts: number;
  channelInteraction: number;
  holdingBrnd: number;
  collectibles: number;
  votedBrands: number;
  sharedPodiums: number;
  neynarScore: number;
  proUser: number;
}

export interface ChallengeBreakdown {
  name: string;
  description: string;
  currentValue: number;
  currentMultiplier: number;
  maxMultiplier: number;
  completed: boolean;
  progress: {
    current: number;
    required: number;
    unit: string;
  };
  tiers: {
    requirement: number;
    multiplier: number;
    achieved: boolean;
  }[];
  details?: {
    [key: string]: any;
  };
}

export interface AirdropCalculation {
  fid: number;
  basePoints: number;
  multipliers: AirdropMultipliers;
  totalMultiplier: number;
  finalScore: number;
  tokenAllocation: number;
  percentage: number;
  leaderboardPosition: number;
  challenges: ChallengeBreakdown[];
  previousScore?: {
    finalScore: number;
    lastUpdated: Date;
  };
}

@Injectable()
export class AirdropService {
  private readonly TOTAL_ALLOCATION = 1_500_000_000;
  private readonly TOP_USERS = 1111;

  constructor(
    @InjectRepository(AirdropScore)
    private readonly airdropScoreRepository: Repository<AirdropScore>,
    @InjectRepository(AirdropSnapshot)
    public readonly airdropSnapshotRepository: Repository<AirdropSnapshot>,
    @InjectRepository(AirdropLeaf)
    private readonly airdropLeafRepository: Repository<AirdropLeaf>,
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
  ) {}

  async checkUserEligibility(fid: number): Promise<AirdropCalculation> {
    const user = await this.userRepository.findOne({ where: { fid } });
    if (!user) {
      throw new Error('User not found');
    }

    // Check if user already has an airdrop score
    const existingAirdropScore = await this.airdropScoreRepository.findOne({
      where: { fid },
      relations: ['user'],
    });

    // STEP 1: Get base points (user's accumulated system points)
    const userSystemPoints = user.points;

    // STEP 2: Calculate all multipliers
    const multiplierData = await this.calculateMultipliersWithBreakdown(fid);

    // STEP 3: Calculate total multiplier
    const totalMultiplier = this.calculateTotalMultiplier(
      multiplierData.multipliers,
    );

    // STEP 4: Calculate final airdrop score
    const airdropScore = Math.round(userSystemPoints * totalMultiplier);

    // Get the actual tokenAllocation and percentage from the saved airdrop score
    let percentage = 0;
    let tokenAllocation = 0;

    if (existingAirdropScore) {
      percentage = Number(existingAirdropScore.percentage) || 0;
      tokenAllocation = Number(existingAirdropScore.tokenAllocation) || 0;
    }

    // STEP 5: Calculate leaderboard position based on new airdrop score
    const leaderboardPosition = await this.getUserLeaderboardPosition(
      fid,
      airdropScore,
    );

    const calculation: AirdropCalculation = {
      fid,
      basePoints: userSystemPoints,
      multipliers: multiplierData.multipliers,
      totalMultiplier,
      finalScore: airdropScore,
      tokenAllocation,
      percentage,
      leaderboardPosition,
      challenges: multiplierData.challenges,
      previousScore: existingAirdropScore
        ? {
            finalScore: Number(existingAirdropScore.finalScore),
            lastUpdated: existingAirdropScore.updatedAt,
          }
        : undefined,
    };

    await this.saveAirdropScore(calculation);

    return calculation;
  }

  private calculateTotalMultiplier(multipliers: AirdropMultipliers): number {
    return Object.values(multipliers).reduce(
      (total, multiplier) => total * multiplier,
      1,
    );
  }

  /**
   * OPTIMIZED: Check user eligibility with pre-fetched data
   * This version avoids redundant API calls and DB queries
   */
  private async checkUserEligibilityOptimized(
    fid: number,
    userSystemPoints: number,
    neynarUserInfoCache: Map<number, any>,
    votedBrandsCount: number,
    sharedPodiumsCount: number,
  ): Promise<AirdropCalculation> {
    // Check if user already has an airdrop score
    const existingAirdropScore = await this.airdropScoreRepository.findOne({
      where: { fid },
    });

    // Calculate multipliers with pre-fetched data
    const multiplierData =
      await this.calculateMultipliersWithBreakdownOptimized(
        fid,
        neynarUserInfoCache,
        votedBrandsCount,
        sharedPodiumsCount,
      );

    // Calculate total multiplier
    const totalMultiplier = this.calculateTotalMultiplier(
      multiplierData.multipliers,
    );

    // Calculate final airdrop score
    const airdropScore = Math.round(userSystemPoints * totalMultiplier);

    // Get token allocation and percentage (will be updated later in bulk)
    const percentage = existingAirdropScore
      ? Number(existingAirdropScore.percentage) || 0
      : 0;
    const tokenAllocation = existingAirdropScore
      ? Number(existingAirdropScore.tokenAllocation) || 0
      : 0;

    // Leaderboard position will be calculated in bulk later
    const leaderboardPosition = 0;

    const calculation: AirdropCalculation = {
      fid,
      basePoints: userSystemPoints,
      multipliers: multiplierData.multipliers,
      totalMultiplier,
      finalScore: airdropScore,
      tokenAllocation,
      percentage,
      leaderboardPosition,
      challenges: multiplierData.challenges,
      previousScore: existingAirdropScore
        ? {
            finalScore: Number(existingAirdropScore.finalScore),
            lastUpdated: existingAirdropScore.updatedAt,
          }
        : undefined,
    };

    // Save airdrop score (without leaderboard position for now)
    await this.saveAirdropScore(calculation);

    return calculation;
  }

  private async getNeynarUserInfo(fid: number): Promise<any> {
    try {
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
      return data?.users?.[0] || null;
    } catch (error) {
      console.error('Error fetching user info:', error);
      return null;
    }
  }

  private async calculateFollowAccountsMultiplier(fid: number): Promise<{
    multiplier: number;
    followedCount: number;
    details: {
      followingBrnd: boolean;
      followingFloc: boolean;
    };
  }> {
    try {
      const apiKey = getConfig().neynar.apiKey.replace(/&$/, '');
      const BRND_FID = 1108951;
      const FLOC_FID = 6946;

      // Check Farcaster follows
      const response = await fetch(
        `https://api.neynar.com/v2/farcaster/user/bulk?fids=${BRND_FID},${FLOC_FID}&viewer_fid=${fid}`,
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

      const users = data?.users || [];
      const brndUser = users.find((u) => u.fid === BRND_FID);
      const flocUser = users.find((u) => u.fid === FLOC_FID);

      const followingBrnd = brndUser?.viewer_context?.following || false;
      const followingFloc = flocUser?.viewer_context?.following || false;

      const details = {
        followingBrnd,
        followingFloc,
      };

      const followedCount = [followingBrnd, followingFloc].filter(
        Boolean,
      ).length;

      let multiplier = 1.0;
      if (followedCount >= 2)
        multiplier = 1.4; // Follow both @brnd and @floc
      else if (followedCount >= 1) multiplier = 1.2; // Follow at least one account

      return { multiplier, followedCount, details };
    } catch (error) {
      console.error('Error calculating follow accounts multiplier:', error);
      return {
        multiplier: 1.0,
        followedCount: 0,
        details: {
          followingBrnd: false,
          followingFloc: false,
        },
      };
    }
  }

  private async checkChannelFollow(fid: number): Promise<boolean> {
    try {
      const apiKey = getConfig().neynar.apiKey.replace(/&$/, '');

      const response = await fetch(
        `https://api.neynar.com/v2/farcaster/channel/search?q=brnd&viewer_fid=${fid}`,
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

      const channel = data?.channels[0] || [];
      const isFollowingBrndChannel =
        channel?.viewer_context?.following || false;

      return isFollowingBrndChannel;
    } catch (error) {
      console.error('Error checking channel follow:', error);
      return false;
    }
  }

  private async checkPodiumCasts(fid: number): Promise<number> {
    try {
      const apiKey = getConfig().neynar.apiKey.replace(/&$/, '');

      const responseCasts = await fetch(
        `https://api.neynar.com/v2/farcaster/feed/user/casts?fid=${fid}&channel_id=brnd&include_replies=false`,
        {
          headers: {
            accept: 'application/json',
            api_key: apiKey,
          },
        },
      );

      if (!responseCasts.ok) {
        throw new Error(
          `Neynar API error: ${responseCasts.status} ${responseCasts.statusText}`,
        );
      }

      const dataCasts = await responseCasts.json();

      const casts = dataCasts?.casts || [];
      const podiumCasts = casts.filter(
        (cast) =>
          Array.isArray(cast.embeds) &&
          cast.embeds.some(
            (embed) =>
              typeof embed.url === 'string' &&
              embed.url.includes('https://brnd.land'),
          ),
      );
      const podiumCastsCount = podiumCasts.length;

      return podiumCastsCount;
    } catch (error) {
      console.error('Error checking podium casts:', error);
      return 0;
    }
  }

  private async calculateChannelInteractionMultiplier(fid: number): Promise<{
    multiplier: number;
    isFollowingChannel: boolean;
    podiumCastsCount: number;
  }> {
    try {
      // Execute both checks in parallel
      const [isFollowingChannel, podiumCastsCount] = await Promise.all([
        this.checkChannelFollow(fid),
        this.checkPodiumCasts(fid),
      ]);

      // Apply multiplier logic according to spec
      let multiplier = 1.0;
      if (podiumCastsCount >= 1 && isFollowingChannel) {
        multiplier = 1.4; // Follow + Publish podium
      } else if (isFollowingChannel) {
        multiplier = 1.2; // Follow channel only
      }

      return { multiplier, isFollowingChannel, podiumCastsCount };
    } catch (error) {
      console.error('Error calculating channel interaction multiplier:', error);
      return {
        multiplier: 1.0,
        isFollowingChannel: false,
        podiumCastsCount: 0,
      };
    }
  }

  private async calculateHoldingMultiplier(fid: number): Promise<{
    multiplier: number;
    totalBalance: number;
    walletBalance: number;
    stakedBalance: number;
  }> {
    try {
      // Get user's verified addresses from Neynar
      const userInfo = await this.getNeynarUserInfo(fid);
      if (!userInfo?.verified_addresses?.eth_addresses) {
        return {
          multiplier: 1.0,
          totalBalance: 0,
          walletBalance: 0,
          stakedBalance: 0,
        };
      }

      const ethAddresses = userInfo.verified_addresses.eth_addresses;

      // Check BRND wallet balance and staked balance for each address in parallel
      const balancePromises = ethAddresses.map(async (address) => {
        const [walletBalance, stakedBalance] = await Promise.all([
          this.getBrndBalance(address),
          this.getStakedBrndBalance(address),
        ]);
        return { walletBalance, stakedBalance };
      });

      const addressBalances = await Promise.all(balancePromises);

      // Sum all wallet and staked balances
      const totalWalletBalance = addressBalances.reduce(
        (sum, balance) => sum + balance.walletBalance,
        0,
      );
      const totalStakedBalance = addressBalances.reduce(
        (sum, balance) => sum + balance.stakedBalance,
        0,
      );
      const totalBalance = totalWalletBalance + totalStakedBalance;

      // Apply multiplier based on total holdings (wallet + staked) according to spec
      let multiplier = 1.0;

      if (totalBalance >= 800_000_000) {
        multiplier = 1.8;
      } else if (totalBalance >= 400_000_000) {
        multiplier = 1.6;
      } else if (totalBalance >= 200_000_000) {
        multiplier = 1.4;
      } else if (totalBalance >= 100_000_000) {
        multiplier = 1.2;
      }

      return {
        multiplier,
        totalBalance,
        walletBalance: totalWalletBalance,
        stakedBalance: totalStakedBalance,
      };
    } catch (error) {
      console.error('Error calculating holdings multiplier:', error);
      return {
        multiplier: 1.0,
        totalBalance: 0,
        walletBalance: 0,
        stakedBalance: 0,
      };
    }
  }

  private async getBrndBalance(address: string): Promise<number> {
    try {
      const BRND_CONTRACT = '0x41Ed0311640A5e489A90940b1c33433501a21B07';
      const appConfig = getConfig();
      const BASE_RPC_URL = appConfig.blockchain.baseRpcUrl;

      // Encode balanceOf call - function selector + padded address
      const functionSelector = '0x70a08231'; // balanceOf(address)
      const paddedAddress = address.slice(2).padStart(64, '0'); // Remove 0x and pad to 64 chars
      const data = functionSelector + paddedAddress;

      const response = await fetch(BASE_RPC_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          jsonrpc: '2.0',
          method: 'eth_call',
          params: [
            {
              to: BRND_CONTRACT,
              data: data,
            },
            'latest',
          ],
          id: 1,
        }),
      });

      if (!response.ok) {
        throw new Error(`RPC error: ${response.status} ${response.statusText}`);
      }

      const result = await response.json();

      if (result.error) {
        throw new Error(`RPC call error: ${result.error.message}`);
      }

      // Convert hex result to number (BRND has 18 decimals)
      const balanceHex = result.result;
      const balanceWei = BigInt(balanceHex);
      const balance = Number(balanceWei) / Math.pow(10, 18);

      return balance;
    } catch (error) {
      console.error(`Error getting BRND balance for ${address}:`, error);
      return 0;
    }
  }

  private async getStakedBrndBalance(address: string): Promise<number> {
    try {
      const TELLER_VAULT = '0x19d1872d8328b23a219e11d3d6eeee1954a88f88';
      const appConfig = getConfig();
      const BASE_RPC_URL = appConfig.blockchain.baseRpcUrl;

      // Step 1: Get vault shares balance (balanceOf on vault contract)
      const functionSelector = '0x70a08231'; // balanceOf(address)
      const paddedAddress = address.slice(2).padStart(64, '0'); // Remove 0x and pad to 64 chars
      const sharesData = functionSelector + paddedAddress;

      const sharesResponse = await fetch(BASE_RPC_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          jsonrpc: '2.0',
          method: 'eth_call',
          params: [
            {
              to: TELLER_VAULT,
              data: sharesData,
            },
            'latest',
          ],
          id: 1,
        }),
      });

      if (!sharesResponse.ok) {
        throw new Error(
          `RPC error: ${sharesResponse.status} ${sharesResponse.statusText}`,
        );
      }

      const sharesResult = await sharesResponse.json();

      if (sharesResult.error) {
        throw new Error(`RPC call error: ${sharesResult.error.message}`);
      }

      const sharesHex = sharesResult.result;
      const sharesBigInt = BigInt(sharesHex);

      // If no shares, return 0
      if (sharesBigInt === 0n) {
        return 0;
      }

      // Step 2: Convert shares to assets using convertToAssets
      const convertToAssetsSelector = '0x07a2d13a'; // convertToAssets(uint256)
      const paddedShares = sharesHex.slice(2).padStart(64, '0'); // Remove 0x and pad to 64 chars
      const convertData = convertToAssetsSelector + paddedShares;

      const assetsResponse = await fetch(BASE_RPC_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          jsonrpc: '2.0',
          method: 'eth_call',
          params: [
            {
              to: TELLER_VAULT,
              data: convertData,
            },
            'latest',
          ],
          id: 2,
        }),
      });

      if (!assetsResponse.ok) {
        throw new Error(
          `RPC error: ${assetsResponse.status} ${assetsResponse.statusText}`,
        );
      }

      const assetsResult = await assetsResponse.json();

      if (assetsResult.error) {
        throw new Error(`RPC call error: ${assetsResult.error.message}`);
      }

      // Convert hex result to number (BRND has 18 decimals)
      const assetsHex = assetsResult.result;
      const assetsBigInt = BigInt(assetsHex);
      const stakedBalance = Number(assetsBigInt) / Math.pow(10, 18);

      return stakedBalance;
    } catch (error) {
      console.error(`Error getting staked BRND balance for ${address}:`, error);
      return 0;
    }
  }

  private async calculateCollectiblesMultiplier(fid: number): Promise<{
    multiplier: number;
    collectiblesCount: number;
  }> {
    try {
      const collectiblesCount = await this.getCollectiblesCount(fid);

      let multiplier = 1.0;

      if (collectiblesCount >= 3) {
        multiplier = 1.8;
      } else if (collectiblesCount >= 2) {
        multiplier = 1.4;
      } else if (collectiblesCount >= 1) {
        multiplier = 1.2;
      }

      return { multiplier, collectiblesCount };
    } catch (error) {
      console.error('Error calculating collectibles multiplier:', error);
      return { multiplier: 1.0, collectiblesCount: 0 };
    }
  }

  private async calculateVotedBrandsMultiplier(fid: number): Promise<{
    multiplier: number;
    votedBrandsCount: number;
  }> {
    try {
      const votedBrandsCount = await this.getVotedBrandsCount(fid);

      let multiplier = 1.0;

      if (votedBrandsCount >= 72) {
        multiplier = 1.8;
      } else if (votedBrandsCount >= 36) {
        multiplier = 1.6;
      } else if (votedBrandsCount >= 18) {
        multiplier = 1.4;
      } else if (votedBrandsCount >= 9) {
        multiplier = 1.2;
      }

      return { multiplier, votedBrandsCount };
    } catch (error) {
      console.error('Error calculating voted brands multiplier:', error);
      return { multiplier: 1.0, votedBrandsCount: 0 };
    }
  }

  private async calculateSharedPodiumsMultiplier(fid: number): Promise<{
    multiplier: number;
    sharedPodiumsCount: number;
  }> {
    try {
      // Query for shared podiums from UserBrandVotes
      const sharedPodiumsCount = await this.getSharedPodiumsCount(fid);

      let multiplier = 1.0;

      if (sharedPodiumsCount >= 80) {
        multiplier = 1.8;
      } else if (sharedPodiumsCount >= 40) {
        multiplier = 1.6;
      } else if (sharedPodiumsCount >= 20) {
        multiplier = 1.4;
      } else if (sharedPodiumsCount >= 10) {
        multiplier = 1.2;
      }

      return { multiplier, sharedPodiumsCount };
    } catch (error) {
      console.error('Error calculating shared podiums multiplier:', error);
      return { multiplier: 1.0, sharedPodiumsCount: 0 };
    }
  }

  private async calculateNeynarScoreMultiplier(fid: number): Promise<{
    multiplier: number;
    neynarScore: number;
  }> {
    try {
      const userInfo = await this.getNeynarUserInfo(fid);
      const neynarScore = userInfo?.score;

      let multiplier = 1.0;

      if (neynarScore >= 1.0) {
        multiplier = 1.8;
      } else if (neynarScore >= 0.9) {
        multiplier = 1.5;
      } else if (neynarScore >= 0.85) {
        multiplier = 1.2;
      }

      return { multiplier, neynarScore };
    } catch (error) {
      console.error('Error calculating Neynar score multiplier:', error);
      return { multiplier: 1.0, neynarScore: 0 };
    }
  }

  private async calculateProUserMultiplier(fid: number): Promise<{
    multiplier: number;
    isProUser: boolean;
    hasBrndTokenInProfile: boolean;
  }> {
    try {
      // Get user info from Neynar to check Pro status and profile
      const userInfo = await this.getNeynarUserInfo(fid);

      if (!userInfo) {
        return {
          multiplier: 1.0,
          isProUser: false,
          hasBrndTokenInProfile: false,
        };
      }

      // Check if user has Pro subscription
      const isProUser =
        userInfo.pro?.status === 'subscribed' &&
        userInfo.pro?.expires_at &&
        new Date(userInfo.pro.expires_at) > new Date();

      // Check if user has BRND token in their profile bio
      const hasBrndTokenInProfile = false;

      let multiplier = 1.0;

      if (isProUser && hasBrndTokenInProfile) {
        multiplier = 1.4;
      } else if (isProUser) {
        multiplier = 1.2;
      }

      return { multiplier, isProUser, hasBrndTokenInProfile };
    } catch (error) {
      console.error('Error calculating Pro User multiplier:', error);
      return {
        multiplier: 1.0,
        isProUser: false,
        hasBrndTokenInProfile: false,
      };
    }
  }

  private async calculateMultipliersWithBreakdown(fid: number): Promise<{
    multipliers: AirdropMultipliers;
    challenges: ChallengeBreakdown[];
  }> {
    // Execute all multiplier calculations in parallel
    const [
      followAccountsResult,
      channelInteractionResult,
      holdingResult,
      collectiblesResult,
      votedBrandsResult,
      sharedPodiumsResult,
      neynarScoreResult,
      proUserResult,
    ] = await Promise.all([
      this.calculateFollowAccountsMultiplier(fid),
      this.calculateChannelInteractionMultiplier(fid),
      this.calculateHoldingMultiplier(fid),
      this.calculateCollectiblesMultiplier(fid),
      this.calculateVotedBrandsMultiplier(fid),
      this.calculateSharedPodiumsMultiplier(fid),
      this.calculateNeynarScoreMultiplier(fid),
      this.calculateProUserMultiplier(fid),
    ]);

    // Build challenges array
    const challenges: ChallengeBreakdown[] = [
      // Follow Accounts Challenge
      {
        name: 'Follow Accounts',
        description: 'Follow @brnd + @floc accounts',
        currentValue: followAccountsResult.followedCount,
        currentMultiplier: followAccountsResult.multiplier,
        maxMultiplier: 1.4,
        completed: followAccountsResult.followedCount >= 2,
        progress: {
          current: followAccountsResult.followedCount,
          required: 2,
          unit: 'following',
        },
        tiers: [
          {
            requirement: 1,
            multiplier: 1.2,
            achieved: followAccountsResult.followedCount >= 1,
          },
          {
            requirement: 2,
            multiplier: 1.4,
            achieved: followAccountsResult.followedCount >= 2,
          },
        ],
        details: {
          accounts: [
            {
              name: '@brnd',
              fid: 1108951,
              followed: followAccountsResult.details.followingBrnd,
              required: true,
            },
            {
              name: '@floc',
              fid: 6946,
              followed: followAccountsResult.details.followingFloc,
              required: true,
            },
          ],
          summary: `${followAccountsResult.followedCount}/2 accounts followed`,
        },
      },

      // Channel Interaction Challenge
      {
        name: 'Channel Interaction /brnd',
        description: 'Follow channel + Publish podiums',
        currentValue: channelInteractionResult.podiumCastsCount,
        currentMultiplier: channelInteractionResult.multiplier,
        maxMultiplier: 1.4,
        completed:
          channelInteractionResult.podiumCastsCount >= 1 &&
          channelInteractionResult.isFollowingChannel,
        progress: {
          current: channelInteractionResult.podiumCastsCount,
          required: 1,
          unit: 'podiums',
        },
        tiers: [
          {
            requirement: 1,
            multiplier: 1.2,
            achieved: channelInteractionResult.isFollowingChannel,
          },
          {
            requirement: 2,
            multiplier: 1.4,
            achieved:
              channelInteractionResult.podiumCastsCount >= 1 &&
              channelInteractionResult.isFollowingChannel,
          },
        ],
        details: {
          channelFollow: {
            channel: '/brnd',
            followed: channelInteractionResult.isFollowingChannel,
            required: true,
          },
          podiumCasts: {
            count: channelInteractionResult.podiumCastsCount,
            required: 1,
            description: 'Publish podium casts in /brnd channel',
          },
          summary: `Channel followed: ${channelInteractionResult.isFollowingChannel ? 'Yes' : 'No'}, Podiums published: ${channelInteractionResult.podiumCastsCount}`,
        },
      },

      // BRND Holdings Challenge
      {
        name: 'Holding $BRND',
        description: 'Hold $BRND',
        currentValue: holdingResult.totalBalance,
        currentMultiplier: holdingResult.multiplier,
        maxMultiplier: 1.8,
        completed: holdingResult.totalBalance >= 800_000_000,
        progress: {
          current: holdingResult.totalBalance,
          required: 800_000_000,
          unit: '$BRND',
        },
        tiers: [
          {
            requirement: 100_000_000,
            multiplier: 1.2,
            achieved: holdingResult.totalBalance >= 100_000_000,
          },
          {
            requirement: 200_000_000,
            multiplier: 1.4,
            achieved: holdingResult.totalBalance >= 200_000_000,
          },
          {
            requirement: 400_000_000,
            multiplier: 1.6,
            achieved: holdingResult.totalBalance >= 400_000_000,
          },
          {
            requirement: 800_000_000,
            multiplier: 1.8,
            achieved: holdingResult.totalBalance >= 800_000_000,
          },
        ],
        details: {
          totalBalance: holdingResult.totalBalance,
          walletBalance: holdingResult.walletBalance,
          stakedBalance: holdingResult.stakedBalance,
          formattedBalance: holdingResult.totalBalance.toLocaleString(),
          formattedWalletBalance: holdingResult.walletBalance.toLocaleString(),
          formattedStakedBalance: holdingResult.stakedBalance.toLocaleString(),
          nextTier:
            holdingResult.totalBalance >= 800_000_000
              ? null
              : holdingResult.totalBalance >= 400_000_000
                ? { requirement: 800_000_000, multiplier: 1.8 }
                : holdingResult.totalBalance >= 200_000_000
                  ? { requirement: 400_000_000, multiplier: 1.6 }
                  : holdingResult.totalBalance >= 100_000_000
                    ? { requirement: 200_000_000, multiplier: 1.4 }
                    : { requirement: 100_000_000, multiplier: 1.2 },
          summary: `Holding ${holdingResult.totalBalance.toLocaleString()} BRND tokens (${holdingResult.walletBalance.toLocaleString()} wallet + ${holdingResult.stakedBalance.toLocaleString()} staked)`,
        },
      },

      // Collectibles Challenge
      {
        name: 'Collect @brndbot casts',
        description: 'Collect @brndbot casts',
        currentValue: collectiblesResult.collectiblesCount,
        currentMultiplier: collectiblesResult.multiplier,
        maxMultiplier: 1.8,
        completed: collectiblesResult.collectiblesCount >= 3,
        progress: {
          current: collectiblesResult.collectiblesCount,
          required: 3,
          unit: 'collectibles',
        },
        tiers: [
          {
            requirement: 1,
            multiplier: 1.2,
            achieved: collectiblesResult.collectiblesCount >= 1,
          },
          {
            requirement: 2,
            multiplier: 1.4,
            achieved: collectiblesResult.collectiblesCount >= 2,
          },
          {
            requirement: 3,
            multiplier: 1.8,
            achieved: collectiblesResult.collectiblesCount >= 3,
          },
        ],
      },

      // Voted Brands Challenge
      {
        name: '# of different brands voted',
        description: 'Vote for different brands',
        currentValue: votedBrandsResult.votedBrandsCount,
        currentMultiplier: votedBrandsResult.multiplier,
        maxMultiplier: 1.8,
        completed: votedBrandsResult.votedBrandsCount >= 72,
        progress: {
          current: votedBrandsResult.votedBrandsCount,
          required: 72,
          unit: 'brands',
        },
        tiers: [
          {
            requirement: 9,
            multiplier: 1.2,
            achieved: votedBrandsResult.votedBrandsCount >= 9,
          },
          {
            requirement: 18,
            multiplier: 1.4,
            achieved: votedBrandsResult.votedBrandsCount >= 18,
          },
          {
            requirement: 36,
            multiplier: 1.6,
            achieved: votedBrandsResult.votedBrandsCount >= 36,
          },
          {
            requirement: 72,
            multiplier: 1.8,
            achieved: votedBrandsResult.votedBrandsCount >= 72,
          },
        ],
        details: {
          uniqueBrandsVoted: votedBrandsResult.votedBrandsCount,
          nextTier:
            votedBrandsResult.votedBrandsCount >= 72
              ? null
              : votedBrandsResult.votedBrandsCount >= 36
                ? { requirement: 72, multiplier: 1.8 }
                : votedBrandsResult.votedBrandsCount >= 18
                  ? { requirement: 36, multiplier: 1.6 }
                  : votedBrandsResult.votedBrandsCount >= 9
                    ? { requirement: 18, multiplier: 1.4 }
                    : { requirement: 9, multiplier: 1.2 },
          summary: `Voted for ${votedBrandsResult.votedBrandsCount} unique brands`,
        },
      },

      // Shared Podiums Challenge
      {
        name: 'Podiums Shared',
        description: 'Shared podiums',
        currentValue: sharedPodiumsResult.sharedPodiumsCount,
        currentMultiplier: sharedPodiumsResult.multiplier,
        maxMultiplier: 1.8,
        completed: sharedPodiumsResult.sharedPodiumsCount >= 80,
        progress: {
          current: sharedPodiumsResult.sharedPodiumsCount,
          required: 80,
          unit: 'podiums',
        },
        tiers: [
          {
            requirement: 10,
            multiplier: 1.2,
            achieved: sharedPodiumsResult.sharedPodiumsCount >= 10,
          },
          {
            requirement: 20,
            multiplier: 1.4,
            achieved: sharedPodiumsResult.sharedPodiumsCount >= 20,
          },
          {
            requirement: 40,
            multiplier: 1.6,
            achieved: sharedPodiumsResult.sharedPodiumsCount >= 40,
          },
          {
            requirement: 80,
            multiplier: 1.8,
            achieved: sharedPodiumsResult.sharedPodiumsCount >= 80,
          },
        ],
        details: {
          sharedPodiumsCount: sharedPodiumsResult.sharedPodiumsCount,
          nextTier:
            sharedPodiumsResult.sharedPodiumsCount >= 80
              ? null
              : sharedPodiumsResult.sharedPodiumsCount >= 40
                ? { requirement: 80, multiplier: 1.8 }
                : sharedPodiumsResult.sharedPodiumsCount >= 20
                  ? { requirement: 40, multiplier: 1.6 }
                  : sharedPodiumsResult.sharedPodiumsCount >= 10
                    ? { requirement: 20, multiplier: 1.4 }
                    : { requirement: 10, multiplier: 1.2 },
          summary: `Shared ${sharedPodiumsResult.sharedPodiumsCount} podiums with castHash`,
        },
      },

      // Neynar Score Challenge
      {
        name: 'Neynar Score',
        description: 'Score de reputación en Neynar',
        currentValue: neynarScoreResult.neynarScore,
        currentMultiplier: neynarScoreResult.multiplier,
        maxMultiplier: 1.8,
        completed: neynarScoreResult.neynarScore >= 1.0,
        progress: {
          current: neynarScoreResult.neynarScore,
          required: 1.0,
          unit: 'score',
        },
        tiers: [
          {
            requirement: 0.85,
            multiplier: 1.2,
            achieved: neynarScoreResult.neynarScore >= 0.85,
          },
          {
            requirement: 0.9,
            multiplier: 1.5,
            achieved: neynarScoreResult.neynarScore >= 0.9,
          },
          {
            requirement: 1.0,
            multiplier: 1.8,
            achieved: neynarScoreResult.neynarScore >= 1.0,
          },
        ],
        details: {
          neynarScore: neynarScoreResult.neynarScore,
          nextTier:
            neynarScoreResult.neynarScore >= 1.0
              ? null
              : neynarScoreResult.neynarScore >= 0.9
                ? { requirement: 1.0, multiplier: 1.8 }
                : neynarScoreResult.neynarScore >= 0.85
                  ? { requirement: 0.9, multiplier: 1.5 }
                  : { requirement: 0.85, multiplier: 1.2 },
          summary: `Neynar score: ${neynarScoreResult.neynarScore}`,
        },
      },

      // Pro User Challenge
      {
        name: 'Pro User',
        description: 'Pro User',
        currentValue: proUserResult.isProUser ? 1 : 0,
        currentMultiplier: proUserResult.multiplier,
        maxMultiplier: 1.4,
        completed: proUserResult.isProUser,
        progress: {
          current: proUserResult.isProUser ? 1 : 0,
          required: 1,
          unit: 'is pro',
        },
        tiers: [
          {
            requirement: 1,
            multiplier: 1.2,
            achieved: proUserResult.isProUser,
          },
        ],
        details: {
          isProUser: proUserResult.isProUser,
          summary: `Pro User: ${proUserResult.isProUser}`,
        },
      },
    ];

    const multipliers: AirdropMultipliers = {
      followAccounts: followAccountsResult.multiplier,
      channelInteraction: channelInteractionResult.multiplier,
      holdingBrnd: holdingResult.multiplier,
      collectibles: collectiblesResult.multiplier,
      votedBrands: votedBrandsResult.multiplier,
      sharedPodiums: sharedPodiumsResult.multiplier,
      neynarScore: neynarScoreResult.multiplier,
      proUser: proUserResult.multiplier,
    };

    return { multipliers, challenges };
  }

  private async getCollectiblesCount(fid: number): Promise<number> {
    try {
      const config = getConfig();
      const indexerUrl = config.collectiblesIndexer?.apiRoute;
      const apiKey = config.collectiblesIndexer?.apiKey;

      if (!indexerUrl || !apiKey) {
        return 0;
      }

      // Query collectibles from specific creator FIDs: 1341847 and 1108951
      const collectedFids = '1341847,1108951';
      const url = `${indexerUrl}/brnd-airdrop/collected-casts?collectorFid=${fid}&collectedFids=${collectedFids}`;

      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'X-API-KEY': apiKey,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        return 0;
      }

      const data = await response.json();
      const collectiblesCount = data?.count || 0;

      return collectiblesCount;
    } catch (error) {
      console.error('Error fetching collectibles count:', error);
      return 0;
    }
  }

  private async getVotedBrandsCount(fid: number): Promise<number> {
    try {
      const user = await this.userRepository.findOne({
        where: { fid },
        relations: [
          'userBrandVotes',
          'userBrandVotes.brand1',
          'userBrandVotes.brand2',
          'userBrandVotes.brand3',
        ],
      });

      if (!user || !user.userBrandVotes) {
        return 0;
      }

      // Collect all unique brand IDs from all three brand positions in each vote
      const uniqueBrands = new Set<number>();

      user.userBrandVotes.forEach((vote) => {
        if (vote.brand1?.id) uniqueBrands.add(vote.brand1.id);
        if (vote.brand2?.id) uniqueBrands.add(vote.brand2.id);
        if (vote.brand3?.id) uniqueBrands.add(vote.brand3.id);
      });

      return uniqueBrands.size;
    } catch (error) {
      console.error('Error getting voted brands count:', error);
      return 0;
    }
  }

  private async getSharedPodiumsCount(fid: number): Promise<number> {
    try {
      const user = await this.userRepository.findOne({
        where: { fid },
        relations: ['userBrandVotes'],
      });

      if (!user || !user.userBrandVotes) {
        return 0;
      }

      // Filter votes that are shared and have a castHash
      const sharedPodiums = user.userBrandVotes.filter(
        (vote) =>
          vote.shared === true &&
          vote.castHash !== null &&
          vote.castHash !== undefined,
      );

      return sharedPodiums.length;
    } catch (error) {
      console.error('Error getting shared podiums count:', error);
      return 0;
    }
  }

  private async getUserLeaderboardPosition(
    fid: number,
    finalScore: number,
  ): Promise<number> {
    try {
      const higherScores = await this.airdropScoreRepository
        .createQueryBuilder('score')
        .where('score.finalScore > :finalScore', { finalScore })
        .getCount();
      return higherScores + 1;
    } catch (error) {
      console.error('Error getting leaderboard position:', error);
      return 0;
    }
  }

  private async saveAirdropScore(
    calculation: AirdropCalculation,
  ): Promise<void> {
    try {
      let airdropScore = await this.airdropScoreRepository.findOne({
        where: { fid: calculation.fid },
      });

      if (!airdropScore) {
        airdropScore = new AirdropScore();
        airdropScore.fid = calculation.fid;
      }

      airdropScore.basePoints = calculation.basePoints;
      airdropScore.followAccountsMultiplier =
        calculation.multipliers.followAccounts;
      airdropScore.channelInteractionMultiplier =
        calculation.multipliers.channelInteraction;
      airdropScore.holdingBrndMultiplier = calculation.multipliers.holdingBrnd;
      airdropScore.collectiblesMultiplier =
        calculation.multipliers.collectibles;
      airdropScore.votedBrandsMultiplier = calculation.multipliers.votedBrands;
      airdropScore.sharedPodiumsMultiplier =
        calculation.multipliers.sharedPodiums;
      airdropScore.neynarScoreMultiplier = calculation.multipliers.neynarScore;
      airdropScore.proUserMultiplier = calculation.multipliers.proUser;
      airdropScore.totalMultiplier = calculation.totalMultiplier;
      airdropScore.finalScore = calculation.finalScore;
      airdropScore.tokenAllocation = calculation.tokenAllocation;
      airdropScore.percentage = calculation.percentage;
      airdropScore.challenges = calculation.challenges || null;

      await this.airdropScoreRepository.save(airdropScore);
    } catch (error) {
      console.error('Error saving airdrop score:', error);
    }
  }

  async getLeaderboard(limit: number = 100): Promise<AirdropScore[]> {
    return this.airdropScoreRepository.find({
      relations: ['user'],
      order: {
        finalScore: 'DESC',
      },
      take: limit,
    });
  }

  async getDatabaseSummary(): Promise<{
    totalUsers: number;
    usersWithVotes: number;
    usersWithSharedPodiums: number;
    totalVotes: number;
    totalSharedPodiums: number;
    averageVotesPerUser: number;
    averageSharedPodiumsPerUser: number;
    averageBrandsVotedPerUser: number;
    existingAirdropScores: number;
    topUsersByPoints: Array<{ fid: number; username: string; points: number }>;
    topUsersByVotes: Array<{
      fid: number;
      username: string;
      votesCount: number;
    }>;
    topUsersBySharedPodiums: Array<{
      fid: number;
      username: string;
      sharedPodiumsCount: number;
    }>;
  }> {
    // Get total users
    const totalUsers = await this.userRepository.count();

    // Get users with votes
    const usersWithVotes = await this.userRepository
      .createQueryBuilder('user')
      .innerJoin('user.userBrandVotes', 'votes')
      .getCount();

    // Get users with shared podiums
    const usersWithSharedPodiums = await this.userRepository
      .createQueryBuilder('user')
      .innerJoin('user.userBrandVotes', 'votes')
      .where('votes.shared = :shared', { shared: true })
      .andWhere('votes.castHash IS NOT NULL')
      .getCount();

    // Get total votes count
    const totalVotesResult = await this.userRepository.query(`
      SELECT COUNT(*) as total FROM user_brand_votes
    `);
    const totalVotes = parseInt(totalVotesResult[0]?.total || '0');

    // Get total shared podiums count
    const totalSharedPodiumsResult = await this.userRepository.query(`
      SELECT COUNT(*) as total FROM user_brand_votes 
      WHERE shared = true AND castHash IS NOT NULL
    `);
    const totalSharedPodiums = parseInt(
      totalSharedPodiumsResult[0]?.total || '0',
    );

    // Calculate averages
    const averageVotesPerUser = totalUsers > 0 ? totalVotes / totalUsers : 0;
    const averageSharedPodiumsPerUser =
      totalUsers > 0 ? totalSharedPodiums / totalUsers : 0;

    // Get average unique brands voted per user
    const brandsVotedResult = await this.userRepository.query(`
      SELECT AVG(unique_brands) as avg_brands FROM (
        SELECT 
          u.fid,
          COUNT(DISTINCT 
            CASE WHEN v.brand1Id IS NOT NULL THEN v.brand1Id END ||
            CASE WHEN v.brand2Id IS NOT NULL THEN v.brand2Id END ||
            CASE WHEN v.brand3Id IS NOT NULL THEN v.brand3Id END
          ) as unique_brands
        FROM users u
        LEFT JOIN user_brand_votes v ON u.id = v.userId
        GROUP BY u.fid
      ) subquery
    `);
    const averageBrandsVotedPerUser = parseFloat(
      brandsVotedResult[0]?.avg_brands || '0',
    );

    // Get existing airdrop scores count
    const existingAirdropScores = await this.airdropScoreRepository.count();

    // Get top users by points
    const topUsersByPoints = await this.userRepository.find({
      select: ['fid', 'username', 'points'],
      order: { points: 'DESC' },
      take: 10,
    });

    // Get top users by votes count
    const topUsersByVotesResult = await this.userRepository.query(`
      SELECT u.fid, u.username, COUNT(v.id) as votesCount
      FROM users u
      LEFT JOIN user_brand_votes v ON u.id = v.userId
      GROUP BY u.fid, u.username
      ORDER BY votesCount DESC
      LIMIT 10
    `);
    const topUsersByVotes = topUsersByVotesResult.map((row) => ({
      fid: row.fid,
      username: row.username,
      votesCount: parseInt(row.votesCount),
    }));

    // Get top users by shared podiums
    const topUsersBySharedPodiumsResult = await this.userRepository.query(`
      SELECT u.fid, u.username, COUNT(v.id) as sharedPodiumsCount
      FROM users u
      LEFT JOIN user_brand_votes v ON u.id = v.userId
      WHERE v.shared = true AND v.castHash IS NOT NULL
      GROUP BY u.fid, u.username
      ORDER BY sharedPodiumsCount DESC
      LIMIT 10
    `);
    const topUsersBySharedPodiums = topUsersBySharedPodiumsResult.map(
      (row) => ({
        fid: row.fid,
        username: row.username,
        sharedPodiumsCount: parseInt(row.sharedPodiumsCount),
      }),
    );

    const summary = {
      totalUsers,
      usersWithVotes,
      usersWithSharedPodiums,
      totalVotes,
      totalSharedPodiums,
      averageVotesPerUser: Math.round(averageVotesPerUser * 100) / 100,
      averageSharedPodiumsPerUser:
        Math.round(averageSharedPodiumsPerUser * 100) / 100,
      averageBrandsVotedPerUser:
        Math.round(averageBrandsVotedPerUser * 100) / 100,
      existingAirdropScores,
      topUsersByPoints: topUsersByPoints.map((user) => ({
        fid: user.fid,
        username: user.username,
        points: user.points,
      })),
      topUsersByVotes,
      topUsersBySharedPodiums,
    };

    return summary;
  }

  async fixZeroScoreAllocations(): Promise<{ updatedUsers: number }> {
    const result = await this.airdropScoreRepository
      .createQueryBuilder()
      .update()
      .set({
        tokenAllocation: 0,
        percentage: 0,
      })
      .where('finalScore = 0 OR finalScore IS NULL')
      .execute();

    return { updatedUsers: result.affected || 0 };
  }

  async recalculateTokenDistribution(): Promise<{
    totalUsers: number;
    totalAirdropPoints: number;
    totalTokensAllocated: number;
    usersWithZeroScore: number;
    distributionStats: {
      under1USD: number;
      under5USD: number;
      under10USD: number;
      under20USD: number;
      over20USD: number;
      over30USD: number;
    };
  }> {
    // STEP 1: Fix zero score allocations first
    await this.fixZeroScoreAllocations();

    // STEP 2: Get all airdrop scores with valid values
    const airdropScores = await this.airdropScoreRepository
      .createQueryBuilder('score')
      .select(['score.fid', 'score.finalScore'])
      .where('score.finalScore > 0')
      .getMany();

    const totalAirdropPoints = airdropScores.reduce(
      (sum, score) => sum + Number(score.finalScore),
      0,
    );

    // Update token allocations for all users using BULK SQL operation
    let totalTokensAllocated = 0;
    const BRND_USD_PRICE = 0.0000009867;
    const distributionStats = {
      under1USD: 0,
      under5USD: 0,
      under10USD: 0,
      under20USD: 0,
      over20USD: 0,
      over30USD: 0,
    };

    // Prepare CASE statements for bulk SQL update
    const fidList: number[] = [];
    const tokenAllocationCases: string[] = [];
    const percentageCases: string[] = [];
    for (const score of airdropScores) {
      const finalScore = Number(score.finalScore);
      const percentage = (finalScore / totalAirdropPoints) * 100;
      const tokenAllocation = Math.round(
        (finalScore / totalAirdropPoints) * this.TOTAL_ALLOCATION,
      );
      const usdValue = tokenAllocation * BRND_USD_PRICE;

      // Validate calculations
      if (isNaN(percentage) || isNaN(tokenAllocation)) {
        console.error(
          `❌ [TOKEN RECALC] Invalid calculation for FID ${score.fid}:`,
          {
            finalScore,
            totalAirdropPoints,
            percentage,
            tokenAllocation,
          },
        );
        continue;
      }

      fidList.push(score.fid);
      tokenAllocationCases.push(`WHEN ${score.fid} THEN ${tokenAllocation}`);
      percentageCases.push(`WHEN ${score.fid} THEN ${percentage}`);

      totalTokensAllocated += tokenAllocation;

      // Categorize by USD value
      if (usdValue < 1) distributionStats.under1USD++;
      else if (usdValue < 5) distributionStats.under5USD++;
      else if (usdValue < 10) distributionStats.under10USD++;
      else if (usdValue < 20) distributionStats.under20USD++;
      else if (usdValue >= 30) distributionStats.over30USD++;
      else distributionStats.over20USD++;
    }

    // Execute BULK update using raw SQL with CASE statements
    try {
      // Get the correct table name from TypeORM metadata
      const tableName = this.airdropScoreRepository.metadata.tableName;

      const sql = `
        UPDATE ${tableName} 
        SET 
          tokenAllocation = CASE fid ${tokenAllocationCases.join(' ')} END,
          percentage = CASE fid ${percentageCases.join(' ')} END,
          updatedAt = NOW()
        WHERE fid IN (${fidList.join(',')})
      `;

      await this.airdropScoreRepository.query(sql);
    } catch (error) {
      console.error(`[AIRDROP] BULK update failed:`, error);
      throw error;
    }

    // Set zero allocation for users with 0 score - do this FIRST
    await this.airdropScoreRepository
      .createQueryBuilder()
      .update()
      .set({
        tokenAllocation: 0,
        percentage: 0,
      })
      .where('finalScore = 0')
      .execute();

    const usersWithZeroScore = await this.airdropScoreRepository
      .createQueryBuilder('score')
      .where('score.finalScore = 0')
      .getCount();

    return {
      totalUsers: airdropScores.length,
      totalAirdropPoints,
      totalTokensAllocated,
      usersWithZeroScore,
      distributionStats,
    };
  }

  async getAirdropAnalytics(): Promise<{
    summary: any;
    usdDistribution: any;
    topUsers: any[];
    bottomUsers: any[];
    statistics: any;
  }> {
    const BRND_USD_PRICE = 0.0000009867;

    // Get all airdrop scores with user info
    const airdropScores = await this.airdropScoreRepository.find({
      relations: ['user'],
      order: { finalScore: 'DESC' },
    });

    // Calculate USD values and statistics
    let totalTokens = 0;
    let totalUSDValue = 0;
    const usdDistribution = {
      under1USD: 0,
      between1_5USD: 0,
      between5_10USD: 0,
      between10_20USD: 0,
      between20_30USD: 0,
      over30USD: 0,
    };

    const usersWithUSD = airdropScores.map((score) => {
      const tokenAllocation = Number(score.tokenAllocation);
      const usdValue = tokenAllocation * BRND_USD_PRICE;
      totalTokens += tokenAllocation;
      totalUSDValue += usdValue;

      // Categorize
      if (usdValue < 1) usdDistribution.under1USD++;
      else if (usdValue < 5) usdDistribution.between1_5USD++;
      else if (usdValue < 10) usdDistribution.between5_10USD++;
      else if (usdValue < 20) usdDistribution.between10_20USD++;
      else if (usdValue < 30) usdDistribution.between20_30USD++;
      else usdDistribution.over30USD++;

      return {
        ...score,
        usdValue: Math.round(usdValue * 100) / 100,
      };
    });

    const topUsers = usersWithUSD.slice(0, 20);
    const bottomUsers = usersWithUSD.slice(-20).reverse();

    const summary = {
      totalUsers: airdropScores.length,
      totalTokensDistributed: totalTokens,
      totalUSDValue: Math.round(totalUSDValue * 100) / 100,
      averageTokensPerUser: Math.round(totalTokens / airdropScores.length),
      averageUSDPerUser:
        Math.round((totalUSDValue / airdropScores.length) * 100) / 100,
      brndUSDPrice: BRND_USD_PRICE,
    };

    const statistics = {
      highestAllocation: {
        tokens: Math.max(...usersWithUSD.map((u) => Number(u.tokenAllocation))),
        usd: Math.max(...usersWithUSD.map((u) => u.usdValue)),
      },
      lowestAllocation: {
        tokens: Math.min(
          ...usersWithUSD
            .filter((u) => Number(u.tokenAllocation) > 0)
            .map((u) => Number(u.tokenAllocation)),
        ),
        usd: Math.min(
          ...usersWithUSD.filter((u) => u.usdValue > 0).map((u) => u.usdValue),
        ),
      },
    };

    return {
      summary,
      usdDistribution,
      topUsers,
      bottomUsers,
      statistics,
    };
  }

  /**
   * OPTIMIZED: Batch fetch Neynar user info for multiple FIDs at once
   * Neynar bulk API supports up to 100 FIDs per request
   */
  private async batchGetNeynarUserInfo(
    fids: number[],
  ): Promise<Map<number, any>> {
    const userInfoMap = new Map<number, any>();
    const apiKey = getConfig().neynar.apiKey.replace(/&$/, '');
    const MAX_FIDS_PER_REQUEST = 100; // Neynar bulk API limit

    // Split into chunks of 100
    for (let i = 0; i < fids.length; i += MAX_FIDS_PER_REQUEST) {
      const chunk = fids.slice(i, i + MAX_FIDS_PER_REQUEST);
      const fidsParam = chunk.join(',');

      try {
        const response = await fetch(
          `https://api.neynar.com/v2/farcaster/user/bulk?fids=${fidsParam}`,
          {
            headers: {
              accept: 'application/json',
              api_key: apiKey,
            },
          },
        );

        if (!response.ok) {
          console.error(
            `Neynar API error for batch: ${response.status} ${response.statusText}`,
          );
          continue;
        }

        const data = await response.json();
        if (data?.users) {
          for (const user of data.users) {
            if (user?.fid) {
              userInfoMap.set(user.fid, user);
            }
          }
        }
      } catch (error) {
        console.error(`Error fetching batch Neynar user info:`, error);
      }
    }

    return userInfoMap;
  }

  /**
   * OPTIMIZED: Pre-fetch all vote data for users in bulk using raw SQL
   * Returns maps for quick lookup: votedBrandsCount and sharedPodiumsCount
   */
  private async preFetchVoteData(fids: number[]): Promise<{
    votedBrandsCountMap: Map<number, number>;
    sharedPodiumsCountMap: Map<number, number>;
  }> {
    const votedBrandsCountMap = new Map<number, number>();
    const sharedPodiumsCountMap = new Map<number, number>();

    if (fids.length === 0) {
      return { votedBrandsCountMap, sharedPodiumsCountMap };
    }

    try {
      const queryRunner =
        this.userRepository.manager.connection.createQueryRunner();

      try {
        // Get unique voted brands count per user using parameterized query
        const votedBrandsQuery = `
          SELECT 
            u.fid,
            COUNT(DISTINCT brand_id) as votedBrandsCount
          FROM users u
          INNER JOIN (
            SELECT userId, brand1Id as brand_id FROM user_brand_votes WHERE brand1Id IS NOT NULL
            UNION
            SELECT userId, brand2Id as brand_id FROM user_brand_votes WHERE brand2Id IS NOT NULL
            UNION
            SELECT userId, brand3Id as brand_id FROM user_brand_votes WHERE brand3Id IS NOT NULL
          ) unique_brands ON u.id = unique_brands.userId
          WHERE u.fid IN (${fids.map(() => '?').join(',')})
          GROUP BY u.fid
        `;

        // Get shared podiums count per user using parameterized query
        const sharedPodiumsQuery = `
          SELECT 
            u.fid,
            COUNT(*) as sharedPodiumsCount
          FROM users u
          INNER JOIN user_brand_votes ubv ON u.id = ubv.userId
          WHERE u.fid IN (${fids.map(() => '?').join(',')})
            AND ubv.shared = 1
            AND ubv.castHash IS NOT NULL
          GROUP BY u.fid
        `;

        // Execute both queries with parameters
        const [votedBrandsResults, sharedPodiumsResults] = await Promise.all([
          queryRunner.query(votedBrandsQuery, fids),
          queryRunner.query(sharedPodiumsQuery, fids),
        ]);

        // Build maps
        for (const row of votedBrandsResults) {
          votedBrandsCountMap.set(row.fid, Number(row.votedBrandsCount) || 0);
        }

        for (const row of sharedPodiumsResults) {
          sharedPodiumsCountMap.set(
            row.fid,
            Number(row.sharedPodiumsCount) || 0,
          );
        }

        // Set 0 for users not found in results
        for (const fid of fids) {
          if (!votedBrandsCountMap.has(fid)) {
            votedBrandsCountMap.set(fid, 0);
          }
          if (!sharedPodiumsCountMap.has(fid)) {
            sharedPodiumsCountMap.set(fid, 0);
          }
        }
      } finally {
        await queryRunner.release();
      }
    } catch (error) {
      console.error('Error pre-fetching vote data:', error);
      // Set defaults on error
      for (const fid of fids) {
        votedBrandsCountMap.set(fid, 0);
        sharedPodiumsCountMap.set(fid, 0);
      }
    }

    return { votedBrandsCountMap, sharedPodiumsCountMap };
  }

  /**
   * OPTIMIZED: Calculate multipliers with pre-fetched data to avoid redundant calls
   */
  private async calculateMultipliersWithBreakdownOptimized(
    fid: number,
    neynarUserInfoCache: Map<number, any>,
    votedBrandsCount: number,
    sharedPodiumsCount: number,
  ): Promise<{
    multipliers: AirdropMultipliers;
    challenges: ChallengeBreakdown[];
  }> {
    // Get cached user info (or fetch if not in cache)
    let userInfo = neynarUserInfoCache.get(fid);
    if (!userInfo) {
      userInfo = await this.getNeynarUserInfo(fid);
      if (userInfo) {
        neynarUserInfoCache.set(fid, userInfo);
      }
    }

    // Execute multiplier calculations in parallel
    const [
      followAccountsResult,
      channelInteractionResult,
      holdingResult,
      collectiblesResult,
      sharedPodiumsResult,
      neynarScoreResult,
      proUserResult,
    ] = await Promise.all([
      this.calculateFollowAccountsMultiplier(fid),
      this.calculateChannelInteractionMultiplier(fid),
      this.calculateHoldingMultiplierOptimized(fid, userInfo),
      this.calculateCollectiblesMultiplier(fid),
      Promise.resolve({
        multiplier:
          this.getSharedPodiumsMultiplierFromCount(sharedPodiumsCount),
        sharedPodiumsCount,
      }),
      this.calculateNeynarScoreMultiplierOptimized(userInfo),
      this.calculateProUserMultiplierOptimized(userInfo),
    ]);

    // Use pre-fetched voted brands count
    const votedBrandsResult = {
      multiplier: this.getVotedBrandsMultiplierFromCount(votedBrandsCount),
      votedBrandsCount,
    };

    // Build challenges array (same structure as before)
    const challenges: ChallengeBreakdown[] = [
      {
        name: 'Follow Accounts',
        description: 'Follow @brnd + @floc accounts',
        currentValue: followAccountsResult.followedCount,
        currentMultiplier: followAccountsResult.multiplier,
        maxMultiplier: 1.4,
        completed: followAccountsResult.followedCount >= 2,
        progress: {
          current: followAccountsResult.followedCount,
          required: 2,
          unit: 'following',
        },
        tiers: [
          {
            requirement: 1,
            multiplier: 1.2,
            achieved: followAccountsResult.followedCount >= 1,
          },
          {
            requirement: 2,
            multiplier: 1.4,
            achieved: followAccountsResult.followedCount >= 2,
          },
        ],
        details: followAccountsResult.details,
      },
      {
        name: 'Channel Interaction',
        description: 'Follow @brnd channel + publish podium casts',
        currentValue: channelInteractionResult.podiumCastsCount,
        currentMultiplier: channelInteractionResult.multiplier,
        maxMultiplier: 1.4,
        completed:
          channelInteractionResult.podiumCastsCount >= 1 &&
          channelInteractionResult.isFollowingChannel,
        progress: {
          current: channelInteractionResult.podiumCastsCount,
          required: 1,
          unit: 'casts',
        },
        tiers: [
          {
            requirement: 1,
            multiplier: 1.2,
            achieved: channelInteractionResult.isFollowingChannel,
          },
          {
            requirement: 1,
            multiplier: 1.4,
            achieved:
              channelInteractionResult.podiumCastsCount >= 1 &&
              channelInteractionResult.isFollowingChannel,
          },
        ],
        details: {
          isFollowingChannel: channelInteractionResult.isFollowingChannel,
          podiumCastsCount: channelInteractionResult.podiumCastsCount,
        },
      },
      {
        name: 'Holding BRND',
        description: 'Hold BRND tokens in wallet or staked',
        currentValue: holdingResult.totalBalance,
        currentMultiplier: holdingResult.multiplier,
        maxMultiplier: 1.8,
        completed: holdingResult.totalBalance >= 800_000_000,
        progress: {
          current: holdingResult.totalBalance,
          required: 800_000_000,
          unit: 'BRND',
        },
        tiers: [
          {
            requirement: 100_000_000,
            multiplier: 1.2,
            achieved: holdingResult.totalBalance >= 100_000_000,
          },
          {
            requirement: 200_000_000,
            multiplier: 1.4,
            achieved: holdingResult.totalBalance >= 200_000_000,
          },
          {
            requirement: 400_000_000,
            multiplier: 1.6,
            achieved: holdingResult.totalBalance >= 400_000_000,
          },
          {
            requirement: 800_000_000,
            multiplier: 1.8,
            achieved: holdingResult.totalBalance >= 800_000_000,
          },
        ],
        details: {
          walletBalance: holdingResult.walletBalance,
          stakedBalance: holdingResult.stakedBalance,
          totalBalance: holdingResult.totalBalance,
        },
      },
      {
        name: 'Collectibles',
        description: 'Collect BRND cast collectibles',
        currentValue: collectiblesResult.collectiblesCount,
        currentMultiplier: collectiblesResult.multiplier,
        maxMultiplier: 1.8,
        completed: collectiblesResult.collectiblesCount >= 3,
        progress: {
          current: collectiblesResult.collectiblesCount,
          required: 3,
          unit: 'collectibles',
        },
        tiers: [
          {
            requirement: 1,
            multiplier: 1.2,
            achieved: collectiblesResult.collectiblesCount >= 1,
          },
          {
            requirement: 2,
            multiplier: 1.4,
            achieved: collectiblesResult.collectiblesCount >= 2,
          },
          {
            requirement: 3,
            multiplier: 1.8,
            achieved: collectiblesResult.collectiblesCount >= 3,
          },
        ],
      },
      {
        name: 'Voted Brands',
        description: 'Vote for unique brands',
        currentValue: votedBrandsResult.votedBrandsCount,
        currentMultiplier: votedBrandsResult.multiplier,
        maxMultiplier: 1.8,
        completed: votedBrandsResult.votedBrandsCount >= 72,
        progress: {
          current: votedBrandsResult.votedBrandsCount,
          required: 72,
          unit: 'brands',
        },
        tiers: [
          {
            requirement: 9,
            multiplier: 1.2,
            achieved: votedBrandsResult.votedBrandsCount >= 9,
          },
          {
            requirement: 18,
            multiplier: 1.4,
            achieved: votedBrandsResult.votedBrandsCount >= 18,
          },
          {
            requirement: 36,
            multiplier: 1.6,
            achieved: votedBrandsResult.votedBrandsCount >= 36,
          },
          {
            requirement: 72,
            multiplier: 1.8,
            achieved: votedBrandsResult.votedBrandsCount >= 72,
          },
        ],
      },
      {
        name: 'Shared Podiums',
        description: 'Share your podium casts',
        currentValue: sharedPodiumsResult.sharedPodiumsCount,
        currentMultiplier: sharedPodiumsResult.multiplier,
        maxMultiplier: 1.8,
        completed: sharedPodiumsResult.sharedPodiumsCount >= 80,
        progress: {
          current: sharedPodiumsResult.sharedPodiumsCount,
          required: 80,
          unit: 'podiums',
        },
        tiers: [
          {
            requirement: 10,
            multiplier: 1.2,
            achieved: sharedPodiumsResult.sharedPodiumsCount >= 10,
          },
          {
            requirement: 20,
            multiplier: 1.4,
            achieved: sharedPodiumsResult.sharedPodiumsCount >= 20,
          },
          {
            requirement: 40,
            multiplier: 1.6,
            achieved: sharedPodiumsResult.sharedPodiumsCount >= 40,
          },
          {
            requirement: 80,
            multiplier: 1.8,
            achieved: sharedPodiumsResult.sharedPodiumsCount >= 80,
          },
        ],
      },
      {
        name: 'Neynar Score',
        description: 'Maintain high Neynar power badge score',
        currentValue: neynarScoreResult.neynarScore,
        currentMultiplier: neynarScoreResult.multiplier,
        maxMultiplier: 1.8,
        completed: neynarScoreResult.neynarScore >= 1.0,
        progress: {
          current: neynarScoreResult.neynarScore,
          required: 1.0,
          unit: 'score',
        },
        tiers: [
          {
            requirement: 0.85,
            multiplier: 1.2,
            achieved: neynarScoreResult.neynarScore >= 0.85,
          },
          {
            requirement: 0.9,
            multiplier: 1.5,
            achieved: neynarScoreResult.neynarScore >= 0.9,
          },
          {
            requirement: 1.0,
            multiplier: 1.8,
            achieved: neynarScoreResult.neynarScore >= 1.0,
          },
        ],
        details: {
          hasPowerBadge: neynarScoreResult.hasPowerBadge,
        },
      },
      {
        name: 'Pro User',
        description: 'Subscribe to Farcaster Pro',
        currentValue: proUserResult.isProUser ? 1 : 0,
        currentMultiplier: proUserResult.multiplier,
        maxMultiplier: 1.4,
        completed:
          proUserResult.isProUser && proUserResult.hasBrndTokenInProfile,
        progress: {
          current: proUserResult.isProUser ? 1 : 0,
          required: 1,
          unit: 'subscription',
        },
        tiers: [
          {
            requirement: 1,
            multiplier: 1.2,
            achieved: proUserResult.isProUser,
          },
          {
            requirement: 1,
            multiplier: 1.4,
            achieved:
              proUserResult.isProUser && proUserResult.hasBrndTokenInProfile,
          },
        ],
        details: {
          isProUser: proUserResult.isProUser,
          hasBrndTokenInProfile: proUserResult.hasBrndTokenInProfile,
        },
      },
    ];

    return {
      multipliers: {
        followAccounts: followAccountsResult.multiplier,
        channelInteraction: channelInteractionResult.multiplier,
        holdingBrnd: holdingResult.multiplier,
        collectibles: collectiblesResult.multiplier,
        votedBrands: votedBrandsResult.multiplier,
        sharedPodiums: sharedPodiumsResult.multiplier,
        neynarScore: neynarScoreResult.multiplier,
        proUser: proUserResult.multiplier,
      },
      challenges,
    };
  }

  /**
   * Helper: Get multiplier from voted brands count
   */
  private getVotedBrandsMultiplierFromCount(count: number): number {
    if (count >= 72) return 1.8;
    if (count >= 36) return 1.6;
    if (count >= 18) return 1.4;
    if (count >= 9) return 1.2;
    return 1.0;
  }

  /**
   * Helper: Get multiplier from shared podiums count
   */
  private getSharedPodiumsMultiplierFromCount(count: number): number {
    if (count >= 80) return 1.8;
    if (count >= 40) return 1.6;
    if (count >= 20) return 1.4;
    if (count >= 10) return 1.2;
    return 1.0;
  }

  /**
   * OPTIMIZED: Calculate holding multiplier with pre-fetched user info
   */
  private async calculateHoldingMultiplierOptimized(
    fid: number,
    userInfo: any,
  ): Promise<{
    multiplier: number;
    totalBalance: number;
    walletBalance: number;
    stakedBalance: number;
  }> {
    try {
      if (!userInfo?.verified_addresses?.eth_addresses) {
        return {
          multiplier: 1.0,
          totalBalance: 0,
          walletBalance: 0,
          stakedBalance: 0,
        };
      }

      const ethAddresses = userInfo.verified_addresses.eth_addresses;
      const balancePromises = ethAddresses.map(async (address: string) => {
        const [walletBalance, stakedBalance] = await Promise.all([
          this.getBrndBalance(address),
          this.getStakedBrndBalance(address),
        ]);
        return { walletBalance, stakedBalance };
      });

      const addressBalances = await Promise.all(balancePromises);
      const totalWalletBalance = addressBalances.reduce(
        (sum, balance) => sum + balance.walletBalance,
        0,
      );
      const totalStakedBalance = addressBalances.reduce(
        (sum, balance) => sum + balance.stakedBalance,
        0,
      );
      const totalBalance = totalWalletBalance + totalStakedBalance;

      let multiplier = 1.0;
      if (totalBalance >= 800_000_000) multiplier = 1.8;
      else if (totalBalance >= 400_000_000) multiplier = 1.6;
      else if (totalBalance >= 200_000_000) multiplier = 1.4;
      else if (totalBalance >= 100_000_000) multiplier = 1.2;

      return {
        multiplier,
        totalBalance,
        walletBalance: totalWalletBalance,
        stakedBalance: totalStakedBalance,
      };
    } catch (error) {
      console.error('Error calculating holdings multiplier:', error);
      return {
        multiplier: 1.0,
        totalBalance: 0,
        walletBalance: 0,
        stakedBalance: 0,
      };
    }
  }

  /**
   * OPTIMIZED: Calculate Neynar score multiplier with pre-fetched user info
   */
  private calculateNeynarScoreMultiplierOptimized(userInfo: any): {
    multiplier: number;
    neynarScore: number;
    hasPowerBadge: boolean;
  } {
    const hasPowerBadge = userInfo?.power_badge || false;
    const neynarScore = userInfo?.score;

    let multiplier = 1.0;
    if (neynarScore >= 1.0) multiplier = 1.8;
    else if (neynarScore >= 0.9) multiplier = 1.5;
    else if (neynarScore >= 0.85) multiplier = 1.2;

    return { multiplier, neynarScore, hasPowerBadge };
  }

  /**
   * OPTIMIZED: Calculate Pro User multiplier with pre-fetched user info
   */
  private calculateProUserMultiplierOptimized(userInfo: any): {
    multiplier: number;
    isProUser: boolean;
    hasBrndTokenInProfile: boolean;
  } {
    if (!userInfo) {
      return {
        multiplier: 1.0,
        isProUser: false,
        hasBrndTokenInProfile: false,
      };
    }

    const isProUser =
      userInfo.pro?.status === 'subscribed' &&
      userInfo.pro?.expires_at &&
      new Date(userInfo.pro.expires_at) > new Date();
    const hasBrndTokenInProfile = false; // TODO: Implement if needed

    let multiplier = 1.0;
    if (isProUser && hasBrndTokenInProfile) multiplier = 1.4;
    else if (isProUser) multiplier = 1.2;

    return { multiplier, isProUser, hasBrndTokenInProfile };
  }

  async calculateAirdropForAllUsers(batchSize: number = 88): Promise<{
    databaseSummary: any;
    eligibleUsers: number;
    totalAirdropPoints: number;
    totalTokensAllocated: number;
    processed: number;
    successful: number;
    failed: number;
    errors: Array<{ fid: number; error: string }>;
    topAirdropScores: Array<{
      fid: number;
      username: string;
      airdropScore: number;
      tokenAllocation: number;
      percentage: number;
    }>;
  }> {
    // STEP 1: Generate database summary first
    const databaseSummary = await this.getDatabaseSummary();

    // STEP 2: Get top 1111 users by points
    const allUsers = await this.userRepository.find({
      select: [
        'fid',
        'username',
        'points',
        'id',
        'dailyStreak',
        'maxDailyStreak',
        'brndPowerLevel',
      ],
      // where: {
      //   fid: Not(In([5431, 6099, 8109, 222144])),
      // },
      order: { points: 'DESC' },
      take: 1115, // 1111
    });

    // Remove users with FIDs 5431, 6099, 8109, 222144 from the users array
    const excludedFids = [5431, 6099, 8109, 222144];
    const users = allUsers.filter((u) => !excludedFids.includes(u.fid));
    console.log('THE USERS NOW ARE: ', users.length);
    // STEP 3: Pre-fetch all vote data in bulk (OPTIMIZATION)
    const allFids = users.map((u) => u.fid);
    const { votedBrandsCountMap, sharedPodiumsCountMap } =
      await this.preFetchVoteData(allFids);

    let processed = 0;
    let successful = 0;
    let failed = 0;
    const errors: Array<{ fid: number; error: string }> = [];
    const airdropCalculations: Array<{
      fid: number;
      username: string;
      airdropScore: number;
    }> = [];

    // STEP 4: Calculate airdrop scores for all eligible users (OPTIMIZED)
    for (let i = 0; i < users.length; i += batchSize) {
      const batch = users.slice(i, i + batchSize);
      const batchFids = batch.map((u) => u.fid);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(users.length / batchSize);

      // OPTIMIZATION: Batch fetch Neynar user info for entire batch
      const neynarUserInfoCache = await this.batchGetNeynarUserInfo(batchFids);

      // Process each user in the batch
      for (const user of batch) {
        try {
          const calculation = await this.checkUserEligibilityOptimized(
            user.fid,
            user.points,
            neynarUserInfoCache,
            votedBrandsCountMap.get(user.fid) || 0,
            sharedPodiumsCountMap.get(user.fid) || 0,
          );

          airdropCalculations.push({
            fid: user.fid,
            username: user.username,
            airdropScore: calculation.finalScore,
          });

          // Calculate current position based on sorted scores (descending)
          // Sort all calculations so far by score descending, then by FID ascending for stability
          const sortedCalculations = [...airdropCalculations].sort((a, b) => {
            if (b.airdropScore !== a.airdropScore) {
              return b.airdropScore - a.airdropScore; // Higher score first
            }
            return a.fid - b.fid; // Tie-breaker: lower FID first
          });

          // Find the index of this user in the sorted array
          const userIndex = sortedCalculations.findIndex(
            (calc) => calc.fid === user.fid,
          );

          // Calculate position accounting for ties: users with same score share position
          // Position = 1 + (number of users with strictly higher score)
          const userScore = calculation.finalScore;
          const usersWithHigherScore = sortedCalculations.filter(
            (calc) => calc.airdropScore > userScore,
          ).length;
          const position = usersWithHigherScore + 1;

          // Calculate position on points leaderboard (users array is already sorted by points DESC)
          // Count how many users have strictly more points than current user
          const usersWithMorePoints = users.filter(
            (u) => u.points > user.points,
          ).length;
          const pointsPosition = usersWithMorePoints + 1;

          console.log(
            `[AIRDROP] FID ${user.fid} (@${user.username}): Airdrop Score: ${calculation.totalMultiplier.toFixed(2)}x${user.points} points=${calculation.finalScore.toLocaleString()}, position #${pointsPosition}/1111 on the $BRND leaderboard. Current streak: ${user.dailyStreak} days. Max streak: ${user.maxDailyStreak} days. BRND Power Level: ${user.brndPowerLevel}.`,
          );

          successful++;
        } catch (error) {
          console.error(
            `[AIRDROP] Failed FID ${user.fid}:`,
            error instanceof Error ? error.message : String(error),
          );
          failed++;
          errors.push({
            fid: user.fid,
            error: error instanceof Error ? error.message : String(error),
          });
        }
        processed++;
      }

      // Reduced delay - only 500ms between batches (was 2000ms)
      if (i + batchSize < users.length) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    // STEP 5: Calculate token distribution
    const totalAirdropPoints = airdropCalculations.reduce(
      (sum, calc) => sum + calc.airdropScore,
      0,
    );

    // Update each user's airdrop score with proper token allocation
    const updatedCalculations = [];
    let totalTokensAllocated = 0;

    for (const calc of airdropCalculations) {
      const percentage = (calc.airdropScore / totalAirdropPoints) * 100;
      const tokenAllocation = Math.round(
        (calc.airdropScore / totalAirdropPoints) * this.TOTAL_ALLOCATION,
      );
      totalTokensAllocated += tokenAllocation;

      // Update the database with the correct token allocation and percentage
      await this.airdropScoreRepository.update(
        { fid: calc.fid },
        {
          tokenAllocation: tokenAllocation,
          percentage: percentage,
        },
      );

      updatedCalculations.push({
        fid: calc.fid,
        username: calc.username,
        airdropScore: calc.airdropScore,
        tokenAllocation: tokenAllocation,
        percentage: Math.round(percentage * 1000) / 1000, // Round to 3 decimal places
      });
    }

    // Sort by airdrop score descending for top list
    const topAirdropScores = updatedCalculations
      .sort((a, b) => b.airdropScore - a.airdropScore)
      .slice(0, 20); // Top 20 for summary

    // Calculate final positions for all users (accounting for ties)
    const sortedByScore = [...updatedCalculations].sort(
      (a, b) => b.airdropScore - a.airdropScore,
    );

    // Log final results for each user with token allocation and position
    for (const calc of sortedByScore) {
      // Calculate position: 1 + (number of users with strictly higher score)
      const usersWithHigherScore = sortedByScore.filter(
        (c) => c.airdropScore > calc.airdropScore,
      ).length;
      const position = usersWithHigherScore + 1;

      console.log(
        `[AIRDROP] FID ${calc.fid} (@${calc.username}) will get ${calc.tokenAllocation.toLocaleString()} $BRND on the airdrop 1 and is #${position}/1111 on the leaderboard.`,
      );
    }

    // STEP 6: Recalculate token distributions (only if no snapshot exists yet)
    const existingSnapshotsCount = await this.airdropSnapshotRepository.count();

    if (existingSnapshotsCount === 0) {
      try {
        await this.recalculateTokenDistribution();
      } catch (error) {
        console.error(
          `[AIRDROP] Failed to recalculate token distribution:`,
          error,
        );
        // Don't throw - allow the function to continue and return results
      }
    }

    return {
      databaseSummary,
      eligibleUsers: users.length,
      totalAirdropPoints,
      totalTokensAllocated,
      processed,
      successful,
      failed,
      errors,
      topAirdropScores,
    };
  }

  /**
   * ------------------------------------------------------------------
   * MERKLE TREE & SNAPSHOT GENERATION
   * Uses proper keccak256(abi.encode(fid, amount)) for smart contract compatibility
   * CRITICAL: Contract uses abi.encode (not abi.encodePacked), which pads values to 32 bytes
   * ------------------------------------------------------------------
   */

  async generateAirdropSnapshot(): Promise<{
    merkleRoot: string;
    totalUsers: number;
    totalTokens: string;
    totalTokensFormatted: string;
    snapshotId: number;
    migratedUsers: number;
  }> {
    // 1. Delete all existing snapshots (leaves will be deleted via CASCADE)
    const existingSnapshotsCount = await this.airdropSnapshotRepository.count();
    if (existingSnapshotsCount > 0) {
      await this.airdropSnapshotRepository
        .createQueryBuilder()
        .delete()
        .execute();
    }

    const TOP_USERS = 1111;

    // 2. Get top users from airdrop scores
    // Assuming getLeaderboard is available in your class context
    const topUsers: AirdropScore[] = await this.getLeaderboard(TOP_USERS);

    if (topUsers.length === 0) {
      throw new Error('No users found in leaderboard');
    }

    // 3. Prepare data for merkle tree and leaves
    let totalTokensFormatted = 0;
    const abiCoder = AbiCoder.defaultAbiCoder();

    const leafData: Array<{
      fid: number;
      baseAmount: number;
      percentage: number;
      finalScore: number;
      rank: number;
      leafHash: string;
    }> = [];

    // Loop through users, calculate baseAmount, and compute the explicit leaf hash
    topUsers.forEach((user, index) => {
      const tokenAllocation = Number(user.tokenAllocation);
      const baseAmount = tokenAllocation; // Use whole number

      totalTokensFormatted += tokenAllocation;

      // --- CRITICAL: MANUAL HASHING FOR CONTRACT COMPATIBILITY ---
      // Corresponds to Solidity: keccak256(abi.encode(fid, baseAmount))
      const encoded = abiCoder.encode(
        ['uint256', 'uint256'],
        [BigInt(user.fid), BigInt(baseAmount)],
      );

      // We produce a 0x-prefixed hex string
      const leafHash = keccak256(encoded);
      // -----------------------------------------------------------

      leafData.push({
        fid: user.fid,
        baseAmount: baseAmount,
        percentage: Number(user.percentage),
        finalScore: Number(user.finalScore),
        rank: index + 1,
        leafHash,
      });
    });

    // 4. Generate Merkle Tree
    // CRITICAL: We must sort the input data by FID first to ensure deterministic ordering
    // when we rebuild the tree later for proofs.
    const sortedLeafData = leafData.sort((a, b) => a.fid - b.fid);

    // Extract just the hashes for the tree generation
    const leafHashes = sortedLeafData.map((x) => x.leafHash);

    // CRITICAL: sortPairs: true makes it compatible with OpenZeppelin's verify()
    const tree = new MerkleTree(leafHashes, keccak256, {
      sortPairs: true,
    });

    const merkleRoot = tree.getHexRoot();

    // 5. Create snapshot record
    const snapshot = this.airdropSnapshotRepository.create({
      merkleRoot,
      totalUsers: leafData.length,
      totalTokens: totalTokensFormatted.toString(),
      totalTokensFormatted: totalTokensFormatted.toString(),
      snapshotDate: new Date(),
      isActive: true,
      isFrozen: false,
    });

    const savedSnapshot = await this.airdropSnapshotRepository.save(snapshot);

    // 7. Bulk insert leaves using raw SQL for maximum performance
    // Using transaction to ensure data integrity
    try {
      await this.airdropLeafRepository.manager.transaction(
        async (transactionalEntityManager) => {
          // Use larger batch size for raw SQL (MySQL can handle ~1000-2000 placeholders per query)
          // Each row has 7 values, so 500 rows = 3500 placeholders (safe limit)
          const BATCH_SIZE = 500;
          const totalBatches = Math.ceil(sortedLeafData.length / BATCH_SIZE);

          for (let i = 0; i < totalBatches; i++) {
            const start = i * BATCH_SIZE;
            const end = Math.min(start + BATCH_SIZE, sortedLeafData.length);
            const batch = sortedLeafData.slice(start, end);

            // Build parameterized query with VALUES clause
            // Format: (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?), ...
            const placeholders: string[] = [];
            const values: any[] = [];

            batch.forEach((leaf) => {
              placeholders.push('(?, ?, ?, ?, ?, ?, ?)');
              values.push(
                savedSnapshot.id, // snapshotId
                leaf.fid, // fid
                leaf.baseAmount, // baseAmount
                leaf.leafHash, // leafHash
                leaf.percentage, // percentage (decimal)
                leaf.rank, // leaderboardRank
                leaf.finalScore, // finalScore (decimal)
              );
            });

            // Note: createdAt will be set automatically by MySQL's DEFAULT CURRENT_TIMESTAMP
            const sql = `
              INSERT INTO airdrop_leaves 
              (snapshotId, fid, baseAmount, leafHash, percentage, leaderboardRank, finalScore)
              VALUES ${placeholders.join(', ')}
            `;

            await transactionalEntityManager.query(sql, values);
          }
        },
      );
    } catch (error) {
      console.error(`❌ [LEAVES] Failed to save leaf records:`, error);
      throw error;
    }

    // 8. Freeze the snapshot
    await this.airdropSnapshotRepository.update(
      { id: savedSnapshot.id },
      { isFrozen: true },
    );

    return {
      merkleRoot,
      totalUsers: leafData.length,
      totalTokens: totalTokensFormatted.toString(),
      totalTokensFormatted: totalTokensFormatted.toString(),
      snapshotId: savedSnapshot.id,
      migratedUsers: leafData.length,
    };
  }

  /**
   * Generates a merkle proof using 'merkletreejs'
   */
  async generateMerkleProof(
    fid: number,
    snapshotId?: number,
  ): Promise<{
    fid: number;
    amount: string;
    proof: string[];
    merkleRoot: string;
    snapshotId: number;
  } | null> {
    // 1. Get the active snapshot and its leaves
    const snapshot = await this.airdropSnapshotRepository.findOne({
      where: snapshotId ? { id: snapshotId } : { isActive: true },
      relations: ['leaves'],
    });

    if (!snapshot) {
      return null;
    }

    // 2. Find the specific user's leaf
    const userLeaf = snapshot.leaves.find((leaf) => leaf.fid === fid);
    if (!userLeaf) {
      return null;
    }

    // 3. Rebuild Merkle Tree
    // CRITICAL: We must sort the leaves exactly as we did during generation
    const sortedLeaves = snapshot.leaves.sort((a, b) => a.fid - b.fid);

    const allLeafHashes = sortedLeaves.map((l) => l.leafHash);

    const tree = new MerkleTree(allLeafHashes, keccak256, {
      sortPairs: true,
    });

    // 4. Verify Root Matches Database
    const rebuiltRoot = tree.getHexRoot();
    if (rebuiltRoot !== snapshot.merkleRoot) {
      console.error(`[AIRDROP] Tree reconstruction failed!`);
      console.error(`Expected root: ${snapshot.merkleRoot}`);
      console.error(`Rebuilt root: ${rebuiltRoot}`);
      throw new Error('Merkle tree reconstruction failed - roots do not match');
    }

    // 5. Generate Proof
    // We use the stored leafHash directly, because that's what the tree is built of.
    const targetLeaf = userLeaf.leafHash;
    const proof = tree.getHexProof(targetLeaf);

    // 6. Local Verification
    const isValid = tree.verify(proof, targetLeaf, rebuiltRoot);

    if (!isValid) {
      console.error(`[AIRDROP] Local verification failed for FID ${fid}`);
      return null;
    }

    return {
      fid,
      amount: userLeaf.baseAmount.toString(),
      proof,
      merkleRoot: snapshot.merkleRoot,
      snapshotId: snapshot.id,
    };
  }
}
