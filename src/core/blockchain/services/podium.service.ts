import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { createPublicClient, http, keccak256, encodeAbiParameters } from 'viem';
import { base } from 'viem/chains';

import { UserBrandVotes } from '../../../models';
import { logger } from '../../../main';

// Podium Contract ABI
const PODIUM_CONTRACT_ABI = [
  {
    inputs: [
      { internalType: 'address', name: '_brndToken', type: 'address' },
      { internalType: 'address', name: '_season2', type: 'address' },
      { internalType: 'address', name: '_backendSigner', type: 'address' },
      {
        internalType: 'address',
        name: '_protocolFeeRecipient',
        type: 'address',
      },
      { internalType: 'address', name: '_escrowWallet', type: 'address' },
    ],
    stateMutability: 'nonpayable',
    type: 'constructor',
  },
  { inputs: [], name: 'AlreadyMinted', type: 'error' },
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
  {
    inputs: [
      { internalType: 'address', name: 'sender', type: 'address' },
      { internalType: 'uint256', name: 'tokenId', type: 'uint256' },
      { internalType: 'address', name: 'owner', type: 'address' },
    ],
    name: 'ERC721IncorrectOwner',
    type: 'error',
  },
  {
    inputs: [
      { internalType: 'address', name: 'operator', type: 'address' },
      { internalType: 'uint256', name: 'tokenId', type: 'uint256' },
    ],
    name: 'ERC721InsufficientApproval',
    type: 'error',
  },
  {
    inputs: [{ internalType: 'address', name: 'approver', type: 'address' }],
    name: 'ERC721InvalidApprover',
    type: 'error',
  },
  {
    inputs: [{ internalType: 'address', name: 'operator', type: 'address' }],
    name: 'ERC721InvalidOperator',
    type: 'error',
  },
  {
    inputs: [{ internalType: 'address', name: 'owner', type: 'address' }],
    name: 'ERC721InvalidOwner',
    type: 'error',
  },
  {
    inputs: [{ internalType: 'address', name: 'receiver', type: 'address' }],
    name: 'ERC721InvalidReceiver',
    type: 'error',
  },
  {
    inputs: [{ internalType: 'address', name: 'sender', type: 'address' }],
    name: 'ERC721InvalidSender',
    type: 'error',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    name: 'ERC721NonexistentToken',
    type: 'error',
  },
  { inputs: [], name: 'Expired', type: 'error' },
  { inputs: [], name: 'InsufficientBalance', type: 'error' },
  { inputs: [], name: 'InvalidInput', type: 'error' },
  { inputs: [], name: 'NotMinted', type: 'error' },
  { inputs: [], name: 'NothingToClaim', type: 'error' },
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
  { inputs: [], name: 'ReentrancyGuardReentrantCall', type: 'error' },
  { inputs: [], name: 'TransferBlocked', type: 'error' },
  { inputs: [], name: 'Unauthorized', type: 'error' },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'owner',
        type: 'address',
      },
      {
        indexed: true,
        internalType: 'address',
        name: 'approved',
        type: 'address',
      },
      {
        indexed: true,
        internalType: 'uint256',
        name: 'tokenId',
        type: 'uint256',
      },
    ],
    name: 'Approval',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'owner',
        type: 'address',
      },
      {
        indexed: true,
        internalType: 'address',
        name: 'operator',
        type: 'address',
      },
      { indexed: false, internalType: 'bool', name: 'approved', type: 'bool' },
    ],
    name: 'ApprovalForAll',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'uint256',
        name: 'tokenId',
        type: 'uint256',
      },
      {
        indexed: true,
        internalType: 'uint256',
        name: 'ownerFid',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'amount',
        type: 'uint256',
      },
    ],
    name: 'FeesClaimed',
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
        internalType: 'uint256',
        name: 'tokenId',
        type: 'uint256',
      },
      {
        indexed: true,
        internalType: 'uint256',
        name: 'newOwnerFid',
        type: 'uint256',
      },
      {
        indexed: true,
        internalType: 'uint256',
        name: 'previousOwnerFid',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'price',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'sellerProceeds',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'genesisRoyalty',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'protocolFee',
        type: 'uint256',
      },
    ],
    name: 'PodiumBought',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'uint256',
        name: 'tokenId',
        type: 'uint256',
      },
      {
        indexed: true,
        internalType: 'bytes32',
        name: 'arrangementHash',
        type: 'bytes32',
      },
      {
        indexed: true,
        internalType: 'uint256',
        name: 'ownerFid',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'uint16[3]',
        name: 'brandIds',
        type: 'uint16[3]',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'price',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'address',
        name: 'wallet',
        type: 'address',
      },
    ],
    name: 'PodiumMinted',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      { indexed: true, internalType: 'uint256', name: 'fid', type: 'uint256' },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'amount',
        type: 'uint256',
      },
    ],
    name: 'ProceedsClaimed',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      { indexed: true, internalType: 'uint256', name: 'fid', type: 'uint256' },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'amount',
        type: 'uint256',
      },
    ],
    name: 'RoyaltiesClaimed',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      { indexed: true, internalType: 'address', name: 'from', type: 'address' },
      { indexed: true, internalType: 'address', name: 'to', type: 'address' },
      {
        indexed: true,
        internalType: 'uint256',
        name: 'tokenId',
        type: 'uint256',
      },
    ],
    name: 'Transfer',
    type: 'event',
  },
  {
    inputs: [],
    name: 'BASE_PRICE',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'BPS_DENOMINATOR',
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
    name: 'GENESIS_ROYALTY_BPS',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'MULTIPLIER_DENOMINATOR',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'PRICE_MULTIPLIER',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'PROTOCOL_FEE_BPS',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'SEASON2',
    outputs: [
      { internalType: 'contract IBRNDSeason2', name: '', type: 'address' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'to', type: 'address' },
      { internalType: 'uint256', name: 'tokenId', type: 'uint256' },
    ],
    name: 'approve',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'bytes32', name: '', type: 'bytes32' }],
    name: 'arrangementToTokenId',
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
    inputs: [{ internalType: 'address', name: 'owner', type: 'address' }],
    name: 'balanceOf',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: 'tokenId', type: 'uint256' },
      { internalType: 'uint256', name: 'buyerFid', type: 'uint256' },
    ],
    name: 'buyPodium',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'claimBalance',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint16[3]', name: 'brandIds', type: 'uint16[3]' },
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'uint256', name: 'deadline', type: 'uint256' },
      { internalType: 'bytes', name: 'signature', type: 'bytes' },
    ],
    name: 'claimPodium',
    outputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: 'tokenId', type: 'uint256' },
      { internalType: 'uint256', name: 'feeAmount', type: 'uint256' },
      { internalType: 'uint256', name: 'deadline', type: 'uint256' },
      { internalType: 'bytes', name: 'signature', type: 'bytes' },
    ],
    name: 'claimRepeatFees',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'claimableProceeds',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'claimableRoyalties',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'escrowWallet',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'feeClaimNonces',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'fidNonces',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    name: 'getApproved',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint16[3]', name: 'brandIds', type: 'uint16[3]' },
    ],
    name: 'getArrangementHash',
    outputs: [{ internalType: 'bytes32', name: '', type: 'bytes32' }],
    stateMutability: 'pure',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    name: 'getPodium',
    outputs: [
      {
        components: [
          { internalType: 'uint16[3]', name: 'brandIds', type: 'uint16[3]' },
          {
            internalType: 'uint256',
            name: 'genesisCreatorFid',
            type: 'uint256',
          },
          { internalType: 'uint256', name: 'ownerFid', type: 'uint256' },
          { internalType: 'uint256', name: 'claimCount', type: 'uint256' },
          { internalType: 'uint256', name: 'lastSalePrice', type: 'uint256' },
          { internalType: 'uint256', name: 'totalFeesEarned', type: 'uint256' },
          { internalType: 'uint256', name: 'createdAt', type: 'uint256' },
        ],
        internalType: 'struct BRNDPodiumCollectables.PodiumData',
        name: '',
        type: 'tuple',
      },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    name: 'getPriceByTokenId',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'owner', type: 'address' },
      { internalType: 'address', name: 'operator', type: 'address' },
    ],
    name: 'isApprovedForAll',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'name',
    outputs: [{ internalType: 'string', name: '', type: 'string' }],
    stateMutability: 'view',
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
    inputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    name: 'ownerOf',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'podiumData',
    outputs: [
      { internalType: 'uint256', name: 'genesisCreatorFid', type: 'uint256' },
      { internalType: 'uint256', name: 'ownerFid', type: 'uint256' },
      { internalType: 'uint256', name: 'claimCount', type: 'uint256' },
      { internalType: 'uint256', name: 'lastSalePrice', type: 'uint256' },
      { internalType: 'uint256', name: 'totalFeesEarned', type: 'uint256' },
      { internalType: 'uint256', name: 'createdAt', type: 'uint256' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'protocolFeeRecipient',
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
    inputs: [
      { internalType: 'address', name: 'from', type: 'address' },
      { internalType: 'address', name: 'to', type: 'address' },
      { internalType: 'uint256', name: 'tokenId', type: 'uint256' },
    ],
    name: 'safeTransferFrom',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'from', type: 'address' },
      { internalType: 'address', name: 'to', type: 'address' },
      { internalType: 'uint256', name: 'tokenId', type: 'uint256' },
      { internalType: 'bytes', name: 'data', type: 'bytes' },
    ],
    name: 'safeTransferFrom',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'operator', type: 'address' },
      { internalType: 'bool', name: 'approved', type: 'bool' },
    ],
    name: 'setApprovalForAll',
    outputs: [],
    stateMutability: 'nonpayable',
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
    inputs: [{ internalType: 'address', name: 'newEscrow', type: 'address' }],
    name: 'setEscrowWallet',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'newRecipient', type: 'address' },
    ],
    name: 'setProtocolFeeRecipient',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'bytes4', name: 'interfaceId', type: 'bytes4' }],
    name: 'supportsInterface',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'symbol',
    outputs: [{ internalType: 'string', name: '', type: 'string' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    name: 'tokenURI',
    outputs: [{ internalType: 'string', name: '', type: 'string' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'totalMinted',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'from', type: 'address' },
      { internalType: 'address', name: 'to', type: 'address' },
      { internalType: 'uint256', name: 'tokenId', type: 'uint256' },
    ],
    name: 'transferFrom',
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
] as const;

@Injectable()
export class PodiumService {
  private readonly PODIUM_CONTRACT_ADDRESS =
    '0x529648D4AC34354F1A37C6fe0f4B6090Ed86fB9e' as `0x${string}`;
  private readonly BASE_PRICE = BigInt('1000000000000000000000000'); // 1,000,000 BRND in wei
  private readonly PRICE_INCREMENT = BigInt('1000000000000000000000000'); // 1,000,000 BRND in wei
  private readonly REPEAT_FEE_BPS = 1000; // 10%

  private readonly publicClient;

  constructor(
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
  ) {
    this.publicClient = createPublicClient({
      chain: base,
      transport: http(),
    });
  }

  // ============================================================================
  //                    COLLECTIBLE EVENT HANDLERS (NEW)
  // ============================================================================

  /**
   * Handle PodiumMinted event from indexer
   * Updates all votes with matching brand combination to mark them as collectibles
   */
  async handleCollectibleMinted(data: {
    tokenId: number;
    brandIds: [number, number, number];
    ownerFid: number;
    ownerWallet: string;
    price: string;
    txHash: string;
  }): Promise<{ affected: number }> {
    try {
      logger.log(
        `🏆 [COLLECTIBLE] Processing mint for Token #${data.tokenId}, Brands: [${data.brandIds.join(', ')}]`,
      );

      const result = await this.userBrandVotesRepository
        .createQueryBuilder()
        .update(UserBrandVotes)
        .set({
          isCollectible: true,
          collectibleTokenId: data.tokenId,
          collectibleOwnerFid: data.ownerFid,
          collectibleOwnerWallet: data.ownerWallet,
          collectiblePrice: data.price,
          collectibleMintTxHash: data.txHash,
          collectibleGenesisCreatorFid: data.ownerFid,
          collectibleClaimCount: 1,
        })
        .where('brand1Id = :b1 AND brand2Id = :b2 AND brand3Id = :b3', {
          b1: data.brandIds[0],
          b2: data.brandIds[1],
          b3: data.brandIds[2],
        })
        .execute();

      logger.log(
        `✅ [COLLECTIBLE] Mint processed - Token #${data.tokenId} - Updated ${result.affected} votes`,
      );

      return { affected: result.affected || 0 };
    } catch (error) {
      logger.error(
        `❌ [COLLECTIBLE] Failed to process mint for Token #${data.tokenId}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Handle PodiumBought event from indexer
   * Updates all votes with matching tokenId to reflect new ownership
   */
  async handleCollectibleBought(data: {
    tokenId: number;
    newOwnerFid: number;
    newOwnerWallet: string;
    price: string;
    claimCount: number;
  }): Promise<{ affected: number }> {
    try {
      logger.log(
        `💰 [COLLECTIBLE] Processing buy for Token #${data.tokenId}, New Owner FID: ${data.newOwnerFid}`,
      );

      const result = await this.userBrandVotesRepository
        .createQueryBuilder()
        .update(UserBrandVotes)
        .set({
          collectibleOwnerFid: data.newOwnerFid,
          collectibleOwnerWallet: data.newOwnerWallet,
          collectiblePrice: data.price,
          collectibleClaimCount: data.claimCount,
        })
        .where('collectibleTokenId = :tokenId', {
          tokenId: data.tokenId,
        })
        .execute();

      logger.log(
        `✅ [COLLECTIBLE] Buy processed - Token #${data.tokenId} - Updated ${result.affected} votes`,
      );

      return { affected: result.affected || 0 };
    } catch (error) {
      logger.error(
        `❌ [COLLECTIBLE] Failed to process buy for Token #${data.tokenId}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Get collectible info for a specific vote/podium arrangement
   */
  async getCollectibleInfo(brandIds: [number, number, number]): Promise<{
    isCollectible: boolean;
    tokenId: number | null;
    ownerFid: number | null;
    ownerWallet: string | null;
    price: string | null;
    genesisCreatorFid: number | null;
    claimCount: number | null;
  }> {
    try {
      const vote = await this.userBrandVotesRepository
        .createQueryBuilder('vote')
        .where(
          'vote.brand1Id = :b1 AND vote.brand2Id = :b2 AND vote.brand3Id = :b3',
          {
            b1: brandIds[0],
            b2: brandIds[1],
            b3: brandIds[2],
          },
        )
        .andWhere('vote.isCollectible = :isCollectible', {
          isCollectible: true,
        })
        .getOne();

      if (!vote) {
        return {
          isCollectible: false,
          tokenId: null,
          ownerFid: null,
          ownerWallet: null,
          price: null,
          genesisCreatorFid: null,
          claimCount: null,
        };
      }

      return {
        isCollectible: true,
        tokenId: vote.collectibleTokenId,
        ownerFid: vote.collectibleOwnerFid,
        ownerWallet: vote.collectibleOwnerWallet,
        price: vote.collectiblePrice,
        genesisCreatorFid: vote.collectibleGenesisCreatorFid,
        claimCount: vote.collectibleClaimCount,
      };
    } catch (error) {
      logger.error('Error getting collectible info:', error);
      throw error;
    }
  }

  // ============================================================================
  //                    EXISTING METHODS (UNCHANGED)
  // ============================================================================

  /**
   * Calculate arrangement hash from brand IDs
   */
  private calculateArrangementHash(
    brandIds: [number, number, number],
  ): `0x${string}` {
    const encoded = encodeAbiParameters(
      [
        { name: 'brand1', type: 'uint16' },
        { name: 'brand2', type: 'uint16' },
        { name: 'brand3', type: 'uint16' },
      ],
      [brandIds[0], brandIds[1], brandIds[2]],
    );
    return keccak256(encoded);
  }

  /**
   * Check if an arrangement is already minted
   */
  async isArrangementMinted(
    brandIds: [number, number, number],
  ): Promise<boolean> {
    try {
      const arrangementHash = this.calculateArrangementHash(brandIds);
      const tokenId = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'arrangementToTokenId',
        args: [arrangementHash],
      });

      return (tokenId as bigint) !== BigInt(0);
    } catch (error) {
      logger.error('Error checking if arrangement is minted:', error);
      throw new Error('Failed to check arrangement mint status');
    }
  }

  /**
   * Get FID nonce from contract
   */
  async getFidNonce(fid: number): Promise<bigint> {
    try {
      const nonce = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'fidNonces',
        args: [BigInt(fid)],
      });

      return nonce as bigint;
    } catch (error) {
      logger.error(`Error getting nonce for FID ${fid}:`, error);
      throw new Error('Failed to get FID nonce from contract');
    }
  }

  /**
   * Get fee claim nonce for a token
   */
  async getFeeClaimNonce(tokenId: string | number): Promise<bigint> {
    try {
      const tokenIdBigInt =
        typeof tokenId === 'string' ? BigInt(tokenId) : BigInt(tokenId);
      const nonce = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'feeClaimNonces',
        args: [tokenIdBigInt],
      });

      return nonce as bigint;
    } catch (error) {
      logger.error(
        `Error getting fee claim nonce for token ${tokenId}:`,
        error,
      );
      throw new Error('Failed to get fee claim nonce from contract');
    }
  }

  /**
   * Get podium data from contract
   */
  async getPodiumData(tokenId: string | number): Promise<{
    ownerFid: bigint;
    claimCount: bigint;
  }> {
    try {
      const tokenIdBigInt =
        typeof tokenId === 'string' ? BigInt(tokenId) : BigInt(tokenId);
      const data = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'podiumData',
        args: [tokenIdBigInt],
      });

      const result = data as readonly [
        bigint,
        bigint,
        bigint,
        bigint,
        bigint,
        bigint,
      ];
      return {
        ownerFid: result[1],
        claimCount: result[2],
      };
    } catch (error) {
      logger.error(`Error getting podium data for token ${tokenId}:`, error);
      throw new Error('Failed to get podium data from contract');
    }
  }

  /**
   * Get current price for an arrangement
   */
  async getCurrentPrice(brandIds: [number, number, number]): Promise<bigint> {
    try {
      const arrangementHash = this.calculateArrangementHash(brandIds);
      const price = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'getCurrentPrice',
        args: [arrangementHash],
      });

      return price as bigint;
    } catch (error) {
      logger.error('Error getting current price:', error);
      const isMinted = await this.isArrangementMinted(brandIds);
      if (!isMinted) {
        return this.BASE_PRICE;
      }
      const arrangementHash = this.calculateArrangementHash(brandIds);
      const tokenId = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'arrangementToTokenId',
        args: [arrangementHash],
      });
      const podiumData = await this.getPodiumData(Number(tokenId as bigint));
      return this.BASE_PRICE + podiumData.claimCount * this.PRICE_INCREMENT;
    }
  }

  /**
   * Calculate price from claim count
   */
  calculatePrice(claimCount: bigint): bigint {
    return this.BASE_PRICE + claimCount * this.PRICE_INCREMENT;
  }

  /**
   * Check if user is eligible to claim a podium
   */
  async checkClaimEligibility(
    fid: number,
    brandIds: [number, number, number],
  ): Promise<{ eligible: boolean; reason: string | null }> {
    try {
      const isMinted = await this.isArrangementMinted(brandIds);
      if (isMinted) {
        return {
          eligible: false,
          reason: 'This podium arrangement has already been minted',
        };
      }

      const votes = await this.userBrandVotesRepository
        .createQueryBuilder('vote')
        .leftJoinAndSelect('vote.user', 'user')
        .leftJoinAndSelect('vote.brand1', 'brand1')
        .leftJoinAndSelect('vote.brand2', 'brand2')
        .leftJoinAndSelect('vote.brand3', 'brand3')
        .where('brand1.id = :brand1', { brand1: brandIds[0] })
        .andWhere('brand2.id = :brand2', { brand2: brandIds[1] })
        .andWhere('brand3.id = :brand3', { brand3: brandIds[2] })
        .orderBy('vote.date', 'DESC')
        .getMany();

      if (votes.length === 0) {
        return {
          eligible: false,
          reason: 'No votes found for this podium arrangement',
        };
      }

      const lastVote = votes[0];
      const userVote = votes.find((vote) => vote.user.fid === fid);

      if (!userVote) {
        return {
          eligible: false,
          reason: 'You have not created or voted this podium arrangement',
        };
      }

      if (lastVote.user.fid !== fid) {
        return {
          eligible: false,
          reason: 'Someone else has voted this arrangement after you',
        };
      }

      return { eligible: true, reason: null };
    } catch (error) {
      logger.error('Error checking claim eligibility:', error);
      return {
        eligible: false,
        reason: 'Error checking eligibility',
      };
    }
  }

  /**
   * Get full podium data including brandIds
   */
  async getPodium(tokenId: string | number): Promise<{
    brandIds: [number, number, number];
    genesisCreatorFid: bigint;
    ownerFid: bigint;
    claimCount: bigint;
    currentPrice: bigint;
    totalFeesEarned: bigint;
    createdAt: bigint;
  }> {
    try {
      const tokenIdBigInt =
        typeof tokenId === 'string' ? BigInt(tokenId) : BigInt(tokenId);
      const data = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'getPodium',
        args: [tokenIdBigInt],
      });

      const result = data as readonly [
        readonly [bigint, bigint, bigint],
        bigint,
        bigint,
        bigint,
        bigint,
        bigint,
        bigint,
      ];
      return {
        brandIds: [
          Number(result[0][0]),
          Number(result[0][1]),
          Number(result[0][2]),
        ] as [number, number, number],
        genesisCreatorFid: result[1],
        ownerFid: result[2],
        claimCount: result[3],
        currentPrice: result[4],
        totalFeesEarned: result[5],
        createdAt: result[6],
      };
    } catch (error) {
      logger.error(`Error getting podium for token ${tokenId}:`, error);
      throw new Error('Failed to get podium data from contract');
    }
  }

  /**
   * Calculate accumulated fees for a podium
   */
  async calculateAccumulatedFees(
    tokenId: string | number,
    feeClaimNonce: bigint,
  ): Promise<bigint> {
    try {
      const podium = await this.getPodium(tokenId);
      const brandIds = podium.brandIds;

      const votes = await this.userBrandVotesRepository
        .createQueryBuilder('vote')
        .leftJoinAndSelect('vote.brand1', 'brand1')
        .leftJoinAndSelect('vote.brand2', 'brand2')
        .leftJoinAndSelect('vote.brand3', 'brand3')
        .where('brand1.id = :brand1', { brand1: brandIds[0] })
        .andWhere('brand2.id = :brand2', { brand2: brandIds[1] })
        .andWhere('brand3.id = :brand3', { brand3: brandIds[2] })
        .orderBy('vote.date', 'ASC')
        .getMany();

      let totalFees = BigInt(0);
      for (const vote of votes) {
        if (vote.brndPaidWhenCreatingPodium) {
          const voteCost = BigInt(vote.brndPaidWhenCreatingPodium);
          const fee = (voteCost * BigInt(this.REPEAT_FEE_BPS)) / BigInt(10000);
          totalFees += fee;
        }
      }

      return totalFees;
    } catch (error) {
      logger.error(`Error calculating fees for token ${tokenId}:`, error);
      throw new Error('Failed to calculate accumulated fees');
    }
  }
}
