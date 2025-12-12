import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { createPublicClient, http, Address } from 'viem';
import { base } from 'viem/chains';

import { User } from '../../../models';
import { getConfig } from '../../../security/config';
import { logger } from '../../../main';

// Contract ABI for BrndSeason2
const BRND_SEASON_2_ABI = [
  {
    inputs: [
      { internalType: 'address', name: '_brndToken', type: 'address' },
      { internalType: 'address', name: '_escrowWallet', type: 'address' },
      { internalType: 'address', name: '_backendSigner', type: 'address' },
    ],
    stateMutability: 'nonpayable',
    type: 'constructor',
  },
  { inputs: [], name: 'AlreadyUsed', type: 'error' },
  { inputs: [], name: 'ECDSAInvalidSignature', type: 'error' },
  {
    inputs: [{ internalType: 'uint256', name: 'length', type: 'uint256' }],
    name: 'ECDSAInvalidSignatureLength',
    type: 'error',
  },
  {
    inputs: [{ internalType: 'bytes32', name: 's', type: 'bytes32' }],
    name: 'ECDSAInvalidSignatureS',
    type: 'error',
  },
  { inputs: [], name: 'Expired', type: 'error' },
  { inputs: [], name: 'InsufficientBalance', type: 'error' },
  { inputs: [], name: 'InvalidInput', type: 'error' },
  {
    inputs: [{ internalType: 'address', name: 'owner', type: 'address' }],
    name: 'OwnableInvalidOwner',
    type: 'error',
  },
  {
    inputs: [{ internalType: 'address', name: 'account', type: 'address' }],
    name: 'OwnableUnauthorizedAccount',
    type: 'error',
  },
  { inputs: [], name: 'Unauthorized', type: 'error' },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'admin',
        type: 'address',
      },
      {
        indexed: true,
        internalType: 'address',
        name: 'addedBy',
        type: 'address',
      },
    ],
    name: 'AdminAdded',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'admin',
        type: 'address',
      },
      {
        indexed: true,
        internalType: 'address',
        name: 'removedBy',
        type: 'address',
      },
    ],
    name: 'AdminRemoved',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'uint16',
        name: 'brandId',
        type: 'uint16',
      },
      {
        indexed: false,
        internalType: 'string',
        name: 'handle',
        type: 'string',
      },
      { indexed: false, internalType: 'uint256', name: 'fid', type: 'uint256' },
      {
        indexed: false,
        internalType: 'address',
        name: 'walletAddress',
        type: 'address',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'createdAt',
        type: 'uint256',
      },
    ],
    name: 'BrandCreated',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'uint16',
        name: 'brandId',
        type: 'uint16',
      },
      { indexed: true, internalType: 'uint256', name: 'fid', type: 'uint256' },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'amount',
        type: 'uint256',
      },
    ],
    name: 'BrandRewardWithdrawn',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'uint16',
        name: 'brandId',
        type: 'uint16',
      },
      {
        indexed: false,
        internalType: 'string',
        name: 'newMetadataHash',
        type: 'string',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'newFid',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'address',
        name: 'newWalletAddress',
        type: 'address',
      },
    ],
    name: 'BrandUpdated',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: 'uint16[]',
        name: 'brandIds',
        type: 'uint16[]',
      },
      {
        indexed: false,
        internalType: 'string[]',
        name: 'handles',
        type: 'string[]',
      },
      {
        indexed: false,
        internalType: 'uint256[]',
        name: 'fids',
        type: 'uint256[]',
      },
      {
        indexed: false,
        internalType: 'address[]',
        name: 'walletAddresses',
        type: 'address[]',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'createdAt',
        type: 'uint256',
      },
    ],
    name: 'BrandsCreated',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      { indexed: true, internalType: 'uint256', name: 'fid', type: 'uint256' },
      { indexed: true, internalType: 'uint8', name: 'newLevel', type: 'uint8' },
      {
        indexed: true,
        internalType: 'address',
        name: 'wallet',
        type: 'address',
      },
    ],
    name: 'BrndPowerLevelUp',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'previousOwner',
        type: 'address',
      },
      {
        indexed: true,
        internalType: 'address',
        name: 'newOwner',
        type: 'address',
      },
    ],
    name: 'OwnershipTransferred',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'voter',
        type: 'address',
      },
      { indexed: true, internalType: 'uint256', name: 'fid', type: 'uint256' },
      { indexed: true, internalType: 'uint256', name: 'day', type: 'uint256' },
      {
        indexed: false,
        internalType: 'uint16[3]',
        name: 'brandIds',
        type: 'uint16[3]',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'cost',
        type: 'uint256',
      },
    ],
    name: 'PodiumCreated',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'recipient',
        type: 'address',
      },
      { indexed: true, internalType: 'uint256', name: 'fid', type: 'uint256' },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'amount',
        type: 'uint256',
      },
      { indexed: false, internalType: 'uint256', name: 'day', type: 'uint256' },
      {
        indexed: false,
        internalType: 'string',
        name: 'castHash',
        type: 'string',
      },
      {
        indexed: false,
        internalType: 'address',
        name: 'caller',
        type: 'address',
      },
    ],
    name: 'RewardClaimed',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      { indexed: true, internalType: 'uint256', name: 'fid', type: 'uint256' },
      {
        indexed: true,
        internalType: 'address',
        name: 'wallet',
        type: 'address',
      },
    ],
    name: 'WalletAuthorized',
    type: 'event',
  },
  {
    inputs: [],
    name: 'BASE_VOTE_COST',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'BRND_TOKEN',
    outputs: [{ internalType: 'contract IBRND', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'ESCROW_WALLET',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'LEVEL_1_VOTE_COST',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'MAX_BRND_POWER_LEVEL',
    outputs: [{ internalType: 'uint8', name: '', type: 'uint8' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'REWARD_MULTIPLIER',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'SECONDS_PER_DAY',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'address', name: 'admin', type: 'address' }],
    name: 'addAdmin',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'address', name: '', type: 'address' }],
    name: 'authorizedFidOf',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'backendSigner',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'string[]', name: 'handles', type: 'string[]' },
      { internalType: 'string[]', name: 'metadataHashes', type: 'string[]' },
      { internalType: 'uint256[]', name: 'fids', type: 'uint256[]' },
      { internalType: 'address[]', name: 'walletAddresses', type: 'address[]' },
    ],
    name: 'batchCreateBrands',
    outputs: [{ internalType: 'uint16[]', name: 'brandIds', type: 'uint16[]' }],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint16', name: '', type: 'uint16' }],
    name: 'brands',
    outputs: [
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'address', name: 'walletAddress', type: 'address' },
      { internalType: 'uint256', name: 'totalBrndAwarded', type: 'uint256' },
      { internalType: 'uint256', name: 'availableBrnd', type: 'uint256' },
      { internalType: 'string', name: 'handle', type: 'string' },
      { internalType: 'string', name: 'metadataHash', type: 'string' },
      { internalType: 'uint256', name: 'createdAt', type: 'uint256' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'address', name: 'account', type: 'address' }],
    name: 'checkIsAdmin',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'recipient', type: 'address' },
      { internalType: 'uint256', name: 'amount', type: 'uint256' },
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'uint256', name: 'day', type: 'uint256' },
      { internalType: 'string', name: 'castHash', type: 'string' },
      { internalType: 'uint256', name: 'deadline', type: 'uint256' },
      { internalType: 'bytes', name: 'signature', type: 'bytes' },
    ],
    name: 'claimReward',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'string', name: 'handle', type: 'string' },
      { internalType: 'string', name: 'metadataHash', type: 'string' },
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'address', name: 'walletAddress', type: 'address' },
    ],
    name: 'createBrand',
    outputs: [{ internalType: 'uint16', name: 'brandId', type: 'uint16' }],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: '', type: 'uint256' },
      { internalType: 'uint256', name: '', type: 'uint256' },
    ],
    name: 'dayFidClaimed',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'dayTotalAllocation',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: '', type: 'uint256' },
      { internalType: 'address', name: '', type: 'address' },
    ],
    name: 'fidNonces',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: '', type: 'uint256' },
      { internalType: 'address', name: '', type: 'address' },
    ],
    name: 'fidWalletAuthorized',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint16', name: 'brandId', type: 'uint16' }],
    name: 'getBrand',
    outputs: [
      {
        components: [
          { internalType: 'uint256', name: 'fid', type: 'uint256' },
          { internalType: 'address', name: 'walletAddress', type: 'address' },
          {
            internalType: 'uint256',
            name: 'totalBrndAwarded',
            type: 'uint256',
          },
          { internalType: 'uint256', name: 'availableBrnd', type: 'uint256' },
          { internalType: 'string', name: 'handle', type: 'string' },
          { internalType: 'string', name: 'metadataHash', type: 'string' },
          { internalType: 'uint256', name: 'createdAt', type: 'uint256' },
        ],
        internalType: 'struct BRNDSeason2.Brand',
        name: '',
        type: 'tuple',
      },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'getCurrentDay',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'uint256', name: 'day', type: 'uint256' },
    ],
    name: 'getDailyPodium',
    outputs: [{ internalType: 'uint16[3]', name: '', type: 'uint16[3]' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint8', name: 'brndPowerLevel', type: 'uint8' }],
    name: 'getRewardAmount',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'getUserInfo',
    outputs: [
      { internalType: 'uint256', name: 'userFid', type: 'uint256' },
      { internalType: 'uint8', name: 'brndPowerLevel', type: 'uint8' },
      { internalType: 'uint32', name: 'lastVoteDay', type: 'uint32' },
      { internalType: 'uint256', name: 'totalVotes', type: 'uint256' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'getUserWallets',
    outputs: [{ internalType: 'address[]', name: '', type: 'address[]' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint8', name: 'brndPowerLevel', type: 'uint8' }],
    name: 'getVoteCost',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'pure',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'string', name: '', type: 'string' }],
    name: 'handleExists',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'uint256', name: 'day', type: 'uint256' },
    ],
    name: 'hasVotedToday',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'address', name: '', type: 'address' }],
    name: 'isAdmin',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'uint8', name: 'newLevel', type: 'uint8' },
      { internalType: 'uint256', name: 'deadline', type: 'uint256' },
      { internalType: 'bytes', name: 'signature', type: 'bytes' },
      { internalType: 'bytes', name: 'authData', type: 'bytes' },
    ],
    name: 'levelUpBrndPower',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [],
    name: 'owner',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'address', name: 'admin', type: 'address' }],
    name: 'removeAdmin',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [],
    name: 'renounceOwnership',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'address', name: '', type: 'address' }],
    name: 'rewardNonces',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'address', name: 'newSigner', type: 'address' }],
    name: 'setBackendSigner',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'address', name: 'newOwner', type: 'address' }],
    name: 'transferOwnership',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint16', name: 'brandId', type: 'uint16' },
      { internalType: 'string', name: 'newMetadataHash', type: 'string' },
      { internalType: 'uint256', name: 'newFid', type: 'uint256' },
      { internalType: 'address', name: 'newWalletAddress', type: 'address' },
    ],
    name: 'updateBrand',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: 'newMultiplier', type: 'uint256' },
    ],
    name: 'updateRewardMultiplier',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'users',
    outputs: [
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'uint8', name: 'brndPowerLevel', type: 'uint8' },
      { internalType: 'uint32', name: 'lastVoteDay', type: 'uint32' },
      { internalType: 'uint256', name: 'totalVotes', type: 'uint256' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint16[3]', name: 'brandIds', type: 'uint16[3]' },
      { internalType: 'bytes', name: 'authData', type: 'bytes' },
    ],
    name: 'vote',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint16', name: 'brandId', type: 'uint16' }],
    name: 'withdrawBrandRewards',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
] as const;

@Injectable()
export class BlockchainService {
  private readonly BRND_TOKEN = '0x41Ed0311640A5e489A90940b1c33433501a21B07';
  private readonly TELLER_VAULT = '0x19d1872d8328b23a219e11d3d6eeee1954a88f88';

  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
  ) {}

  private getPublicClient() {
    const config = getConfig();
    return createPublicClient({
      chain: base,
      transport: http(config.blockchain.baseRpcUrl),
    });
  }

  async getUserStakeInfo(fid: number): Promise<{
    walletBalance: number;
    vaultShares: number;
    stakedAmount: number;
    totalBalance: number;
    addresses: string[];
  }> {
    try {
      logger.log(`💰 [BLOCKCHAIN] Getting stake info for FID: ${fid}`);

      const userInfo = await this.getNeynarUserInfo(fid);
      if (!userInfo?.verified_addresses?.eth_addresses) {
        logger.log(
          `❌ [BLOCKCHAIN] No verified ETH addresses found for FID: ${fid}`,
        );
        return {
          walletBalance: 0,
          vaultShares: 0,
          stakedAmount: 0,
          totalBalance: 0,
          addresses: [],
        };
      }

      const ethAddresses = userInfo.verified_addresses.eth_addresses;
      logger.log(
        `🔍 [BLOCKCHAIN] Found ${ethAddresses.length} verified ETH addresses`,
      );

      const balancePromises = ethAddresses.map(async (address) => {
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

      return {
        walletBalance: totalWalletBalance,
        vaultShares: 0, // TODO: Calculate actual vault shares
        stakedAmount: totalStakedBalance,
        totalBalance: totalWalletBalance + totalStakedBalance,
        addresses: ethAddresses,
      };
    } catch (error) {
      logger.error('Error getting user stake info:', error);
      throw error;
    }
  }

  private async getNeynarUserInfo(fid: number): Promise<any> {
    try {
      logger.log(`🔍 [NEYNAR] Fetching user info for FID: ${fid}`);
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
      logger.error('Error fetching Neynar user info:', error);
      return null;
    }
  }

  private async getBrndBalance(address: string): Promise<number> {
    try {
      logger.log(`🔍 [BLOCKCHAIN] Checking BRND balance for: ${address}`);

      const config = getConfig();
      const BASE_RPC_URL = config.blockchain.baseRpcUrl;

      const functionSelector = '0x70a08231'; // balanceOf(address)
      const paddedAddress = address.slice(2).padStart(64, '0');
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
              to: this.BRND_TOKEN,
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

      const balanceHex = result.result;
      const balanceWei = BigInt(balanceHex);
      const balance = Number(balanceWei) / Math.pow(10, 18);

      logger.log(`💰 [BLOCKCHAIN] BRND balance: ${balance.toLocaleString()}`);
      return balance;
    } catch (error) {
      logger.error(`Error getting BRND balance for ${address}:`, error);
      return 0;
    }
  }

  private async getStakedBrndBalance(address: string): Promise<number> {
    try {
      logger.log(`🥩 [BLOCKCHAIN] Checking staked BRND for: ${address}`);

      const config = getConfig();
      const BASE_RPC_URL = config.blockchain.baseRpcUrl;

      // Step 1: Get vault shares
      const functionSelector = '0x70a08231'; // balanceOf(address)
      const paddedAddress = address.slice(2).padStart(64, '0');
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
              to: this.TELLER_VAULT,
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

      if (sharesBigInt === 0n) {
        logger.log(`🥩 [BLOCKCHAIN] No vault shares for: ${address}`);
        return 0;
      }

      // Step 2: Convert shares to assets
      const convertToAssetsSelector = '0x07a2d13a'; // convertToAssets(uint256)
      const paddedShares = sharesHex.slice(2).padStart(64, '0');
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
              to: this.TELLER_VAULT,
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

      const assetsHex = assetsResult.result;
      const assetsBigInt = BigInt(assetsHex);
      const stakedBalance = Number(assetsBigInt) / Math.pow(10, 18);

      logger.log(
        `🥩 [BLOCKCHAIN] Staked balance: ${stakedBalance.toLocaleString()}`,
      );
      return stakedBalance;
    } catch (error) {
      logger.error(`Error getting staked BRND balance for ${address}:`, error);
      return 0;
    }
  }

  async checkFollowStatus(fid: number): Promise<{
    followingBrnd: boolean;
    followingFloc: boolean;
  }> {
    try {
      logger.log(`🔍 [BLOCKCHAIN] Checking follow status for FID: ${fid}`);
      const apiKey = getConfig().neynar.apiKey.replace(/&$/, '');
      const BRND_FID = 1108951;
      const FLOC_FID = 6946;

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

      logger.log(`📱 [BLOCKCHAIN] Follow status:`, {
        followingBrnd,
        followingFloc,
      });

      return { followingBrnd, followingFloc };
    } catch (error) {
      logger.error('Error checking follow status:', error);
      return { followingBrnd: false, followingFloc: false };
    }
  }

  async getUserInfoFromContractByFid(fid: number): Promise<{
    fid: number;
    brndPowerLevel: number;
    lastVoteDay: number;
    totalVotes: number;
    authorizedWallets: string[];
  } | null> {
    try {
      logger.log(
        `📋 [BLOCKCHAIN] Getting user info from V5 contract for FID: ${fid}`,
      );

      const CONTRACT_ADDRESS = process.env.BRND_SEASON_2_ADDRESS;

      if (!CONTRACT_ADDRESS) {
        throw new Error('BRND_SEASON_2_ADDRESS environment variable not set');
      }

      const publicClient = this.getPublicClient();

      // Call getUserInfo(uint256 fid) using viem
      const result = (await publicClient.readContract({
        address: CONTRACT_ADDRESS as Address,
        abi: BRND_SEASON_2_ABI,
        functionName: 'getUserInfo',
        args: [BigInt(fid)],
      } as any)) as [bigint, number, number, bigint];

      // result is a tuple: [userFid, brndPowerLevel, lastVoteDay, totalVotes]
      const [userFid, brndPowerLevel, lastVoteDay, totalVotes] = result;

      // If FID is 0, user doesn't exist
      if (userFid === 0n) {
        return null;
      }

      // Get authorized wallets for this FID
      const wallets = await this.getUserWalletsFromContract(fid);

      logger.log(`✅ [BLOCKCHAIN] Contract user info for FID ${fid}:`, {
        fid: Number(userFid),
        brndPowerLevel: Number(brndPowerLevel),
        lastVoteDay: Number(lastVoteDay),
        totalVotes: Number(totalVotes),
        walletCount: wallets.length,
      });

      return {
        fid: Number(userFid),
        brndPowerLevel: Number(brndPowerLevel),
        lastVoteDay: Number(lastVoteDay),
        totalVotes: Number(totalVotes),
        authorizedWallets: wallets,
      };
    } catch (error) {
      logger.error(
        `Error getting user info from contract for FID ${fid}:`,
        error,
      );
      return null;
    }
  }

  async getUserWalletsFromContract(fid: number): Promise<string[]> {
    try {
      const CONTRACT_ADDRESS = process.env.BRND_SEASON_2_ADDRESS;

      if (!CONTRACT_ADDRESS) {
        return [];
      }

      const publicClient = this.getPublicClient();

      // Call getUserWallets(uint256 fid) using viem
      const wallets = (await publicClient.readContract({
        address: CONTRACT_ADDRESS as Address,
        abi: BRND_SEASON_2_ABI,
        functionName: 'getUserWallets',
        args: [BigInt(fid)],
      } as any)) as Address[];

      // wallets is an array of addresses
      return wallets.map((wallet) => wallet.toLowerCase());
    } catch (error) {
      logger.error(
        `Error getting user wallets from contract for FID ${fid}:`,
        error,
      );
      return [];
    }
  }

  async getBrandFromContract(brandId: number): Promise<{
    fid: number;
    walletAddress: string;
    totalBrndAwarded: number;
    availableBrnd: number;
    handle: string;
    metadataHash: string;
    createdAt: number;
  } | null> {
    try {
      logger.log(
        `📋 [BLOCKCHAIN] Getting brand from contract for ID: ${brandId}`,
      );

      const CONTRACT_ADDRESS = getConfig().blockchain.contractAddress;
      console.log('THE CONTRACT ADDRESS IS: ', CONTRACT_ADDRESS);
      const publicClient = this.getPublicClient();

      // Call getBrand(uint16 brandId) using viem
      const result = (await publicClient.readContract({
        address: CONTRACT_ADDRESS as Address,
        abi: BRND_SEASON_2_ABI,
        functionName: 'getBrand',
        args: [brandId],
      } as any)) as {
        fid: bigint;
        walletAddress: Address;
        totalBrndAwarded: bigint;
        availableBrnd: bigint;
        handle: string;
        metadataHash: string;
        createdAt: bigint;
      };

      // result is a Brand struct with named properties
      const {
        fid,
        walletAddress,
        totalBrndAwarded,
        availableBrnd,
        handle,
        metadataHash,
        createdAt,
      } = result;

      logger.log(`✅ [BLOCKCHAIN] Contract brand info for ID ${brandId}:`, {
        fid: Number(fid),
        walletAddress,
        handle,
        metadataHash,
        createdAt: Number(createdAt),
      });

      return {
        fid: Number(fid),
        walletAddress: walletAddress.toLowerCase(),
        totalBrndAwarded: Number(totalBrndAwarded),
        availableBrnd: Number(availableBrnd),
        handle,
        metadataHash,
        createdAt: Number(createdAt),
      };
    } catch (error) {
      logger.error(
        `Error getting brand from contract for ID ${brandId}:`,
        error,
      );
      return null;
    }
  }

  async fetchMetadataFromIpfs(ipfsHash: string): Promise<any> {
    try {
      logger.log(`📡 [IPFS] Fetching metadata from hash: ${ipfsHash}`);

      // Try multiple IPFS gateways for redundancy
      const gateways = [
        `https://ipfs.io/ipfs/${ipfsHash}`,
        `https://cloudflare-ipfs.com/ipfs/${ipfsHash}`,
        `https://gateway.pinata.cloud/ipfs/${ipfsHash}`,
      ];

      for (const gateway of gateways) {
        try {
          const response = await fetch(gateway);

          if (response.ok) {
            const metadata = await response.json();
            logger.log(
              `✅ [IPFS] Successfully fetched metadata from: ${gateway}`,
            );
            return metadata;
          }
        } catch (gatewayError) {
          logger.warn(
            `❌ [IPFS] Gateway failed: ${gateway}`,
            gatewayError.message,
          );
        }
      }

      throw new Error('All IPFS gateways failed');
    } catch (error) {
      logger.error(
        `Error fetching metadata from IPFS hash ${ipfsHash}:`,
        error,
      );
      throw error;
    }
  }
}
