import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, In } from 'typeorm';
import {
  createWalletClient,
  createPublicClient,
  http,
  isAddress,
  keccak256,
  stringToBytes,
} from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { base } from 'viem/chains';

import { Brand } from '../../../models';
import { getConfig } from '../../../security/config';
import { logger } from '../../../main';
import { IpfsService } from '../../../utils/ipfs.service';

// Contract ABI for BRNDSEASON1
const CONTRACT_ABI = [
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
        internalType: 'struct StoriesInMotionV7.Brand',
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
    stateMutability: 'pure',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'getUserBrndPowerLevel',
    outputs: [{ internalType: 'uint8', name: '', type: 'uint8' }],
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
    inputs: [{ internalType: 'address', name: 'wallet', type: 'address' }],
    name: 'getUserInfoByWallet',
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
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'getUserTotalVotes',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
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
  {
    inputs: [{ internalType: 'string', name: '', type: 'string' }],
    name: 'handleExists',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
] as const;

interface ContractBrand {
  id: number; // Database ID for tracking
  handle: string;
  metadataHash: string;
  fid: number;
  walletAddress: string;
}

interface ValidationResult {
  valid: boolean;
  issues: string[];
}

interface UploadResult {
  batchesProcessed: number;
  successfulBrands: number;
  failedBrands: number;
  totalGasUsed: number;
  txHashes: string[];
  errors: Array<{
    batch: number;
    brands: string[];
    error: string;
  }>;
}

interface UploadSummary {
  totalBrands: number;
  batchesProcessed: number;
  successfulBrands: number;
  failedBrands: number;
  gasUsed: number;
  transactionHashes: string[];
}

@Injectable()
export class ContractUploadService {
  private readonly BATCH_SIZE = 20; // Reduced for safety
  private readonly DEFAULT_WALLET =
    '0x0000000000000000000000000000000000000000';

  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    private readonly ipfsService: IpfsService,
  ) {}

  async getAllBrandsForContract(limit?: number): Promise<ContractBrand[]> {
    try {
      logger.log(
        `📋 [CONTRACT] Fetching ${limit || 'all'} brands from database for contract upload`,
      );

      const queryOptions: any = {
        select: [
          'id',
          'name',
          'onChainHandle',
          'metadataHash',
          'onChainFid',
          'walletAddress',
          'isUploadedToContract',
        ],
        where: {
          isUploadedToContract: false, // Only get non-uploaded brands
        },
        order: { id: 'ASC' }, // Order by database ID for consistent contract IDs
      };

      if (limit) {
        queryOptions.take = limit;
      }

      const brands = await this.brandRepository.find(queryOptions);

      logger.log(`📋 [CONTRACT] Found ${brands.length} brands in database`);

      // Transform to required format and remove duplicates
      const contractBrands: ContractBrand[] = [];
      const seenHandles = new Set<string>();

      // Process brands sequentially to handle async metadata hash generation
      for (let index = 0; index < brands.length; index++) {
        const brand = brands[index];

        // Use existing handle or name as fallback
        const handle =
          brand.onChainHandle ||
          brand.name.toLowerCase().replace(/[^a-z0-9]/g, '');

        // Skip if we've already seen this handle
        const handleLower = handle.toLowerCase();
        if (seenHandles.has(handleLower)) {
          logger.warn(
            `⚠️  [CONTRACT] Skipping duplicate handle "${handle}" (DB ID: ${brand.id})`,
          );
          continue;
        }
        seenHandles.add(handleLower);

        // Generate metadata hash if missing (upload to IPFS)
        let metadataHash = brand.metadataHash;
        if (!metadataHash) {
          try {
            // Load full brand data with relations for metadata generation
            const fullBrand = await this.brandRepository.findOne({
              where: { id: brand.id },
              relations: ['category'],
            });

            if (!fullBrand) {
              throw new Error(`Brand with ID ${brand.id} not found`);
            }

            metadataHash = await this.generateMetadataHash(fullBrand, index);
            logger.log(
              `📤 [IPFS] Uploaded metadata for brand "${brand.name}" (ID: ${brand.id}): ${metadataHash}`,
            );
          } catch (error) {
            logger.error(
              `❌ [IPFS] Failed to upload metadata for brand "${brand.name}" (ID: ${brand.id}):`,
              error.message,
            );
            // Fallback to generating a hash locally if IPFS upload fails
            // Load minimal brand data for fallback hash
            const minimalBrand = await this.brandRepository.findOne({
              where: { id: brand.id },
              relations: ['category'],
            });
            if (minimalBrand) {
              metadataHash = this.generateLocalMetadataHash(
                minimalBrand,
                index,
              );
              logger.warn(
                `⚠️  [IPFS] Using local hash fallback for brand "${brand.name}": ${metadataHash}`,
              );
            } else {
              // Last resort: use brand name and index
              metadataHash = keccak256(
                stringToBytes(JSON.stringify({ name: brand.name, index })),
              );
              logger.error(
                `❌ [IPFS] Could not load brand data, using minimal hash for "${brand.name}": ${metadataHash}`,
              );
            }
          }
        }

        // Use existing FID or generate placeholder
        const fid = brand.onChainFid || 10000 + index; // Start at 10000 for placeholder FIDs

        // Use existing wallet or default
        const walletAddress = brand.walletAddress || this.DEFAULT_WALLET;

        contractBrands.push({
          id: brand.id,
          handle,
          metadataHash,
          fid,
          walletAddress,
        });
      }

      logger.log(
        `✅ [CONTRACT] Transformed ${contractBrands.length} unique brands for contract (removed ${brands.length - contractBrands.length} duplicates)`,
      );
      return contractBrands;
    } catch (error) {
      logger.error('Error fetching brands for contract:', error);
      throw error;
    }
  }

  validateBrandsForContract(brands: ContractBrand[]): ValidationResult {
    const issues: string[] = [];
    const seenHandles = new Set<string>();

    brands.forEach((brand, index) => {
      // Check required fields
      if (!brand.handle || brand.handle.trim() === '') {
        issues.push(`Brand ${index + 1}: Missing handle`);
      }

      if (!brand.metadataHash || brand.metadataHash.trim() === '') {
        issues.push(`Brand ${index + 1}: Missing metadataHash`);
      }

      if (!brand.fid || brand.fid === 0) {
        issues.push(`Brand ${index + 1}: Missing or invalid fid`);
      }

      if (!brand.walletAddress || !isAddress(brand.walletAddress)) {
        issues.push(
          `Brand ${index + 1}: Invalid wallet address "${brand.walletAddress}"`,
        );
      }

      // Check for duplicates
      const handleLower = brand.handle.toLowerCase();
      if (seenHandles.has(handleLower)) {
        issues.push(`Brand ${index + 1}: Duplicate handle "${brand.handle}"`);
      }
      seenHandles.add(handleLower);

      // Validate handle format
      if (!/^[a-zA-Z0-9_-]+$/.test(brand.handle)) {
        issues.push(
          `Brand ${index + 1}: Invalid handle format "${brand.handle}"`,
        );
      }

      // Check handle length
      if (brand.handle.length > 32) {
        issues.push(
          `Brand ${index + 1}: Handle too long "${brand.handle}" (max 32 characters)`,
        );
      }
    });

    return {
      valid: issues.length === 0,
      issues,
    };
  }

  async resetUploadFlags(): Promise<void> {
    try {
      logger.log(
        '🔄 [CONTRACT] Resetting all upload flags for fresh contract deployment',
      );

      // Get all brand IDs first, then update with specific criteria
      const brands = await this.brandRepository.find({
        select: ['id'],
        where: { isUploadedToContract: true },
      });

      if (brands.length > 0) {
        const brandIds = brands.map((brand) => brand.id);
        await this.brandRepository.update(
          { id: In(brandIds) },
          { isUploadedToContract: false },
        );
        logger.log(
          `✅ [CONTRACT] Reset upload flags for ${brands.length} brands`,
        );
      } else {
        logger.log('✅ [CONTRACT] No brands found with upload flags to reset');
      }
    } catch (error) {
      logger.error('Error resetting upload flags:', error);
      throw error;
    }
  }

  async markBrandsAsUploaded(brandIds: number[]): Promise<void> {
    try {
      await this.brandRepository.update(
        { id: In(brandIds) },
        { isUploadedToContract: true },
      );
      logger.log(
        `✅ [CONTRACT] Marked ${brandIds.length} brands as uploaded: [${brandIds.join(', ')}]`,
      );
    } catch (error) {
      logger.error('Error marking brands as uploaded:', error);
      throw error;
    }
  }

  async uploadBrandsToContract(
    brands: ContractBrand[],
    resetFlags: boolean = true,
  ): Promise<UploadResult> {
    const results: UploadResult = {
      batchesProcessed: 0,
      successfulBrands: 0,
      failedBrands: 0,
      totalGasUsed: 0,
      txHashes: [],
      errors: [],
    };

    try {
      // Initialize viem clients
      const config = getConfig();

      if (!process.env.ADMIN_PRIVATE_KEY) {
        throw new Error('ADMIN_PRIVATE_KEY environment variable not set');
      }

      if (!process.env.BRND_SEASON_2_ADDRESS) {
        throw new Error('BRND_SEASON_2_ADDRESS environment variable not set');
      }

      const privateKey = process.env.ADMIN_PRIVATE_KEY.startsWith('0x')
        ? (process.env.ADMIN_PRIVATE_KEY as `0x${string}`)
        : (`0x${process.env.ADMIN_PRIVATE_KEY}` as `0x${string}`);

      const account = privateKeyToAccount(privateKey);

      const publicClient = createPublicClient({
        chain: base,
        transport: http(config.blockchain.baseRpcUrl),
      });

      const walletClient = createWalletClient({
        account,
        chain: base,
        transport: http(config.blockchain.baseRpcUrl),
      });

      const contractAddress = process.env
        .BRND_SEASON_2_ADDRESS as `0x${string}`;

      // Reset upload flags for fresh contract deployment
      if (resetFlags) {
        await this.resetUploadFlags();
      }

      logger.log(
        `🚀 [CONTRACT] Starting upload of ${brands.length} brands in batches of ${this.BATCH_SIZE}`,
      );
      logger.log(`🔑 [CONTRACT] Using admin wallet: ${account.address}`);
      logger.log(`📋 [CONTRACT] Contract address: ${contractAddress}`);

      // Initialize nonce management - will fetch fresh for each transaction
      logger.log(
        `🔢 [CONTRACT] Using dynamic nonce management for reliability`,
      );

      // Process in batches
      for (let i = 0; i < brands.length; i += this.BATCH_SIZE) {
        const batchEnd = Math.min(i + this.BATCH_SIZE, brands.length);
        const batch = brands.slice(i, batchEnd);
        const batchNumber = results.batchesProcessed + 1;

        logger.log(
          `🔄 [CONTRACT] Processing batch ${batchNumber}: brands ${i + 1} to ${batchEnd}`,
        );

        try {
          // Prepare batch arrays
          const handles = batch.map((b) => b.handle);
          const metadataHashes = batch.map((b) => b.metadataHash);
          const fids = batch.map((b) => BigInt(b.fid));
          const walletAddresses = batch.map(
            (b) => b.walletAddress as `0x${string}`,
          );

          logger.log(`📝 [CONTRACT] Batch ${batchNumber} handles:`, handles);

          // Estimate gas first
          const gasEstimate = await publicClient.estimateContractGas({
            address: contractAddress,
            abi: CONTRACT_ABI,
            functionName: 'batchCreateBrands',
            args: [handles, metadataHashes, fids, walletAddresses],
            account,
          });

          logger.log(
            `⛽ [CONTRACT] Batch ${batchNumber} gas estimate:`,
            gasEstimate.toString(),
          );

          // Get fresh nonce for this transaction
          const currentNonce = await publicClient.getTransactionCount({
            address: account.address,
            blockTag: 'pending', // Include pending transactions
          });

          logger.log(
            `🔢 [CONTRACT] Batch ${batchNumber} using nonce: ${currentNonce}`,
          );

          // Execute transaction with fresh nonce
          const hash = await walletClient.writeContract({
            address: contractAddress,
            abi: CONTRACT_ABI,
            functionName: 'batchCreateBrands',
            args: [handles, metadataHashes, fids, walletAddresses],
            gas: (gasEstimate * 120n) / 100n, // Add 20% buffer
            nonce: currentNonce,
          } as any);

          logger.log(
            `📤 [CONTRACT] Batch ${batchNumber} transaction sent: ${hash}`,
          );

          // Wait for confirmation
          const receipt = await publicClient.waitForTransactionReceipt({
            hash,
          });

          results.batchesProcessed++;
          results.successfulBrands += batch.length;
          results.totalGasUsed += Number(receipt.gasUsed);
          results.txHashes.push(receipt.transactionHash);

          // Mark brands as uploaded in database
          const brandIds = batch.map((brand) => brand.id);
          await this.markBrandsAsUploaded(brandIds);

          logger.log(
            `✅ [CONTRACT] Batch ${batchNumber} successful: ${receipt.transactionHash}`,
          );
          logger.log(
            `⛽ [CONTRACT] Batch ${batchNumber} gas used: ${receipt.gasUsed.toString()}`,
          );

          // Small delay between batches to avoid rate limiting
          if (i + this.BATCH_SIZE < brands.length) {
            logger.log(`⏳ [CONTRACT] Waiting 3 seconds before next batch...`);
            await new Promise((resolve) => setTimeout(resolve, 3000));
          }
        } catch (error) {
          logger.error(
            `❌ [CONTRACT] Batch ${batchNumber} failed:`,
            error.message,
          );

          results.failedBrands += batch.length;
          results.errors.push({
            batch: batchNumber,
            brands: batch.map((b) => b.handle),
            error: error.message,
          });

          // Continue with next batch instead of stopping
        }
      }

      logger.log(
        `🏁 [CONTRACT] Upload complete! ${results.successfulBrands}/${brands.length} brands uploaded successfully`,
      );
      return results;
    } catch (error) {
      logger.error('Critical error in uploadBrandsToContract:', error);
      throw error;
    }
  }

  async checkIfBrandExistsOnContract(handle: string): Promise<boolean> {
    try {
      logger.log(
        `🔍 [CONTRACT] Checking handleExists("${handle}") on contract...`,
      );

      const config = getConfig();

      if (!process.env.BRND_SEASON_2_ADDRESS) {
        throw new Error('BRND_SEASON_2_ADDRESS environment variable not set');
      }

      const publicClient = createPublicClient({
        chain: base,
        transport: http(config.blockchain.baseRpcUrl),
      });

      const contractAddress = process.env
        .BRND_SEASON_2_ADDRESS as `0x${string}`;

      logger.log(
        `🔍 [CONTRACT] Calling handleExists("${handle}") on contract ${contractAddress}`,
      );

      const exists = await publicClient.readContract({
        address: contractAddress,
        abi: CONTRACT_ABI,
        functionName: 'handleExists',
        args: [handle],
      } as any);

      logger.log(`🔍 [CONTRACT] handleExists("${handle}") returned: ${exists}`);

      return Boolean(exists);
    } catch (error) {
      logger.error(
        `Error checking if brand ${handle} exists on contract:`,
        error.message,
      );
      logger.error(error);
      throw error;
    }
  }

  async getContractBrandCount(): Promise<number> {
    try {
      const config = getConfig();

      if (!process.env.BRND_SEASON_2_ADDRESS) {
        throw new Error('BRND_SEASON_2_ADDRESS environment variable not set');
      }

      const publicClient = createPublicClient({
        chain: base,
        transport: http(config.blockchain.baseRpcUrl),
      });

      const contractAddress = process.env
        .BRND_SEASON_2_ADDRESS as `0x${string}`;

      // Since _brandIdCounter is private in V5, we'll count by checking existing brands
      // Try to get brands starting from ID 1 until we hit an error (brand doesn't exist)
      let brandCount = 0;
      let currentId = 1;
      let maxChecks = 1000; // Safety limit to avoid infinite loop

      while (maxChecks > 0) {
        try {
          await publicClient.readContract({
            address: contractAddress,
            abi: CONTRACT_ABI,
            functionName: 'getBrand',
            args: [currentId],
          } as any);

          brandCount++;
          currentId++;
          maxChecks--;
        } catch (error) {
          // Brand doesn't exist, we've found our count
          break;
        }
      }

      logger.log(
        `📊 [CONTRACT] Contract has ${brandCount} brands (checked up to ID ${currentId - 1})`,
      );
      return brandCount;
    } catch (error) {
      logger.error('Error getting contract brand count:', error);
      throw error;
    }
  }

  async getDatabaseBrandCount(): Promise<number> {
    try {
      const count = await this.brandRepository.count();
      logger.log(`📊 [DATABASE] Database has ${count} brands`);
      return count;
    } catch (error) {
      logger.error('Error getting database brand count:', error);
      throw error;
    }
  }

  async getUploadedBrandCount(): Promise<number> {
    try {
      const count = await this.brandRepository.count({
        where: { isUploadedToContract: true },
      });
      logger.log(`📊 [DATABASE] Database has ${count} uploaded brands`);
      return count;
    } catch (error) {
      logger.error('Error getting uploaded brand count:', error);
      throw error;
    }
  }

  async syncExistingBrandsFromContract(): Promise<{
    checkedBrands: number;
    markedAsUploaded: number;
    brandHandles: string[];
  }> {
    try {
      // Get all non-uploaded brands from database
      const nonUploadedBrands = await this.brandRepository.find({
        where: { isUploadedToContract: false },
        select: ['id', 'name', 'onChainHandle'],
      });

      let markedCount = 0;
      const markedHandles: string[] = [];
      const brandIdsToMark: number[] = [];

      logger.log(
        `🔍 [CONTRACT] Checking ${nonUploadedBrands.length} non-uploaded brands against contract`,
      );

      // Check each brand against the contract
      for (const brand of nonUploadedBrands) {
        const handle =
          brand.onChainHandle ||
          brand.name.toLowerCase().replace(/[^a-z0-9]/g, '');

        logger.log(
          `🔍 [CONTRACT] Checking if brand "${handle}" (DB ID: ${brand.id}) exists on contract...`,
        );

        try {
          const existsOnContract =
            await this.checkIfBrandExistsOnContract(handle);

          if (existsOnContract) {
            logger.log(
              `✅ [CONTRACT] Found existing brand on contract: ${handle} (DB ID: ${brand.id})`,
            );
            brandIdsToMark.push(brand.id);
            markedHandles.push(handle);
            markedCount++;
          } else {
            logger.log(
              `❌ [CONTRACT] Brand "${handle}" does NOT exist on contract (will need upload)`,
            );
          }
        } catch (error) {
          logger.error(
            `⚠️  [CONTRACT] Error checking brand ${handle}:`,
            error.message,
          );
          logger.error(error);
          // Continue with next brand
        }
      }

      logger.log(
        `🔍 [CONTRACT] Finished checking all brands. Found ${markedCount} existing brands to mark as uploaded.`,
      );

      // Mark found brands as uploaded in batches
      if (brandIdsToMark.length > 0) {
        await this.markBrandsAsUploaded(brandIdsToMark);
        logger.log(
          `✅ [CONTRACT] Marked ${markedCount} existing brands as uploaded in database`,
        );
      }

      return {
        checkedBrands: nonUploadedBrands.length,
        markedAsUploaded: markedCount,
        brandHandles: markedHandles,
      };
    } catch (error) {
      logger.error('Error syncing existing brands from contract:', error);
      throw error;
    }
  }

  async testUploadSingleBrand(handle: string): Promise<{
    success: boolean;
    error?: string;
    txHash?: string;
  }> {
    try {
      logger.log(`🧪 [CONTRACT] Testing upload of single brand: ${handle}`);

      const config = getConfig();

      if (!process.env.ADMIN_PRIVATE_KEY) {
        throw new Error('ADMIN_PRIVATE_KEY environment variable not set');
      }

      if (!process.env.BRND_SEASON_2_ADDRESS) {
        throw new Error('BRND_SEASON_2_ADDRESS environment variable not set');
      }

      const privateKey = process.env.ADMIN_PRIVATE_KEY.startsWith('0x')
        ? (process.env.ADMIN_PRIVATE_KEY as `0x${string}`)
        : (`0x${process.env.ADMIN_PRIVATE_KEY}` as `0x${string}`);

      const account = privateKeyToAccount(privateKey);

      const publicClient = createPublicClient({
        chain: base,
        transport: http(config.blockchain.baseRpcUrl),
      });

      const walletClient = createWalletClient({
        account,
        chain: base,
        transport: http(config.blockchain.baseRpcUrl),
      });

      const contractAddress = process.env
        .BRND_SEASON_2_ADDRESS as `0x${string}`;

      const brand = await this.brandRepository.findOne({
        where: { onChainHandle: handle },
        relations: ['category'],
      });

      if (!brand) {
        throw new Error(`Brand with handle "${handle}" not found`);
      }

      // Test individual brand upload
      const testMetadataHash = await this.generateMetadataHash(brand, 999);
      const testFid = 99999;
      const testWallet = this.DEFAULT_WALLET;

      logger.log(
        `🧪 [CONTRACT] Attempting to upload: handle=${handle}, fid=${testFid}, wallet=${testWallet}`,
      );

      const hash = await walletClient.writeContract({
        address: contractAddress,
        abi: CONTRACT_ABI,
        functionName: 'batchCreateBrands',
        args: [
          [handle],
          [testMetadataHash],
          [BigInt(testFid)],
          [testWallet as `0x${string}`],
        ],
      } as any);

      // Wait for confirmation
      const receipt = await publicClient.waitForTransactionReceipt({
        hash,
      });

      logger.log(
        `✅ [CONTRACT] Successfully uploaded test brand ${handle}: ${receipt.transactionHash}`,
      );

      return {
        success: true,
        txHash: receipt.transactionHash,
      };
    } catch (error) {
      logger.error(
        `❌ [CONTRACT] Failed to upload test brand ${handle}:`,
        error.message,
      );
      return {
        success: false,
        error: error.message,
      };
    }
  }

  /**
   * Uploads brand metadata to IPFS using Pinata and returns the IPFS hash
   * Uses the same format as NFT metadata uploads
   */
  private async generateMetadataHash(
    brand: Brand,
    index: number,
  ): Promise<string> {
    // Create metadata object in the same format as NFT metadata
    const metadata = {
      name: brand.name,
      url: brand.url || '',
      warpcastUrl: brand.warpcastUrl || '',
      description: brand.description || '',
      categoryId: brand.category?.id || null,
      followerCount: brand.followerCount || 0,
      imageUrl: brand.imageUrl || '',
      profile: brand.profile || '',
      channel: brand.channel || '',
      queryType: brand.queryType ?? null,
      channelOrProfile: brand.queryType === 0 ? 'channel' : 'profile',
      founderFid: brand.founderFid || null,
      ticker: brand.ticker || null,
      contractAddress: brand.contractAddress || null,
      createdAt: brand.createdAt?.toISOString() || new Date().toISOString(),
    };

    // Upload to IPFS using Pinata
    const ipfsHash = await this.ipfsService.uploadJsonToIpfs(metadata);

    logger.log(
      `✅ [IPFS] Successfully uploaded metadata for brand "${brand.name}" to IPFS: ${ipfsHash}`,
    );

    return ipfsHash;
  }

  /**
   * Fallback method to generate a local hash if IPFS upload fails
   * This maintains backward compatibility
   */
  private generateLocalMetadataHash(brand: Brand, index: number): string {
    // Generate a simple metadata hash based on brand name and index
    const metadata = JSON.stringify({
      name: brand.name,
      url: brand.url,
      category: brand.category,
      profile: brand.profile,
      channel: brand.channel,
      description: brand.description,
      founder: brand.founderFid,
      ticker: brand.ticker,
      contract_address: brand.contractAddress,
    });

    return keccak256(stringToBytes(metadata));
  }

  async checkContractStatus(): Promise<{
    database: { totalBrands: number };
    contract: { totalBrands: number; nextBrandId: number };
    sync: { needsUpload: boolean; difference: number };
  }> {
    try {
      const [dbCount, contractCount] = await Promise.all([
        this.getDatabaseBrandCount(),
        this.getContractBrandCount(),
      ]);

      return {
        database: {
          totalBrands: dbCount,
        },
        contract: {
          totalBrands: contractCount,
          nextBrandId: contractCount + 1,
        },
        sync: {
          needsUpload: dbCount > contractCount,
          difference: dbCount - contractCount,
        },
      };
    } catch (error) {
      logger.error('Error checking contract status:', error);
      throw error;
    }
  }
}
