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
  { inputs: [], name: 'ERC721EnumerableForbiddenBatchMint', type: 'error' },
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
  {
    inputs: [
      { internalType: 'address', name: 'owner', type: 'address' },
      { internalType: 'uint256', name: 'index', type: 'uint256' },
    ],
    name: 'ERC721OutOfBoundsIndex',
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
  { inputs: [], name: 'ReentrancyGuard', type: 'error' },
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
      {
        indexed: false,
        internalType: 'address',
        name: 'wallet',
        type: 'address',
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
      {
        indexed: false,
        internalType: 'address',
        name: 'wallet',
        type: 'address',
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
      {
        indexed: false,
        internalType: 'address',
        name: 'wallet',
        type: 'address',
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
    name: 'PRICE_INCREMENT',
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
    name: 'REPEAT_FEE_BPS',
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
      { internalType: 'uint256', name: 'deadline', type: 'uint256' },
      { internalType: 'bytes', name: 'signature', type: 'bytes' },
    ],
    name: 'buyPodium',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'claimAll',
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
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'claimProceeds',
    outputs: [],
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
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'claimRoyalties',
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
    inputs: [
      { internalType: 'address', name: 'token', type: 'address' },
      { internalType: 'uint256', name: 'amount', type: 'uint256' },
    ],
    name: 'emergencyWithdraw',
    outputs: [],
    stateMutability: 'nonpayable',
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
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'getClaimableBalances',
    outputs: [
      { internalType: 'uint256', name: 'proceeds', type: 'uint256' },
      { internalType: 'uint256', name: 'royalties', type: 'uint256' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'bytes32', name: 'arrangementHash', type: 'bytes32' },
    ],
    name: 'getCurrentPrice',
    outputs: [{ internalType: 'uint256', name: 'price', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    name: 'getPodium',
    outputs: [
      { internalType: 'uint16[3]', name: 'brandIds', type: 'uint16[3]' },
      { internalType: 'uint256', name: 'genesisCreatorFid', type: 'uint256' },
      { internalType: 'uint256', name: 'ownerFid', type: 'uint256' },
      { internalType: 'uint256', name: 'claimCount', type: 'uint256' },
      { internalType: 'uint256', name: 'currentPrice', type: 'uint256' },
      { internalType: 'uint256', name: 'totalFeesEarned', type: 'uint256' },
      { internalType: 'uint256', name: 'createdAt', type: 'uint256' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint16[3]', name: 'brandIds', type: 'uint16[3]' },
    ],
    name: 'getTokenIdForArrangement',
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
    inputs: [
      { internalType: 'uint16[3]', name: 'brandIds', type: 'uint16[3]' },
    ],
    name: 'isArrangementMinted',
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
    inputs: [{ internalType: 'uint256', name: 'index', type: 'uint256' }],
    name: 'tokenByIndex',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'owner', type: 'address' },
      { internalType: 'uint256', name: 'index', type: 'uint256' },
    ],
    name: 'tokenOfOwnerByIndex',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'tokenToArrangement',
    outputs: [{ internalType: 'bytes32', name: '', type: 'bytes32' }],
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
    name: 'totalPodiums',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'totalSupply',
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
    '0xe14A1b3f3314De3EBadBc30bFB3a91D4aC49Bd06' as `0x${string}`;
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

      // If tokenId is 0, the arrangement is not minted
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
  async getFeeClaimNonce(tokenId: number): Promise<bigint> {
    try {
      const nonce = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'feeClaimNonces',
        args: [BigInt(tokenId)],
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
  async getPodiumData(tokenId: number): Promise<{
    brand1: number;
    brand2: number;
    brand3: number;
    ownerFid: bigint;
    claimCount: bigint;
  }> {
    try {
      const data = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'podiumData',
        args: [BigInt(tokenId)],
      });

      return {
        brand1: Number((data as any).brand1),
        brand2: Number((data as any).brand2),
        brand3: Number((data as any).brand3),
        ownerFid: (data as any).ownerFid as bigint,
        claimCount: (data as any).claimCount as bigint,
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
      // Fallback to calculating price from claimCount
      const isMinted = await this.isArrangementMinted(brandIds);
      if (!isMinted) {
        return this.BASE_PRICE;
      }
      // If minted, we need to get the tokenId and then podiumData
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
   * Eligibility rules:
   * 1. Arrangement must not be minted
   * 2. User created this podium arrangement in Season2, OR
   * 3. User was the last person to vote this exact arrangement in Season2
   * 4. No one else has voted this arrangement since the user created/voted it
   */
  async checkClaimEligibility(
    fid: number,
    brandIds: [number, number, number],
  ): Promise<{ eligible: boolean; reason: string | null }> {
    try {
      // Check if arrangement is already minted
      const isMinted = await this.isArrangementMinted(brandIds);
      if (isMinted) {
        return {
          eligible: false,
          reason: 'This podium arrangement has already been minted',
        };
      }

      // Find all votes for this exact arrangement using QueryBuilder
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

      // Check if the user created or was the last to vote this arrangement
      const lastVote = votes[0];
      const userVote = votes.find((vote) => vote.user.fid === fid);

      if (!userVote) {
        return {
          eligible: false,
          reason: 'You have not created or voted this podium arrangement',
        };
      }

      // Check if user was the last to vote (or created it)
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
   * Calculate accumulated fees for a podium
   * Fees are 10% of all vote costs for votes on this podium arrangement
   * since the last fee claim
   */
  async calculateAccumulatedFees(
    tokenId: number,
    feeClaimNonce: bigint,
  ): Promise<bigint> {
    try {
      const podiumData = await this.getPodiumData(tokenId);
      const brandIds: [number, number, number] = [
        podiumData.brand1,
        podiumData.brand2,
        podiumData.brand3,
      ];

      // Find all votes for this arrangement using QueryBuilder
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

      // Calculate fees: 10% of each vote cost
      // Note: voteCost is stored as brndPaidWhenCreatingPodium
      let totalFees = BigInt(0);
      for (const vote of votes) {
        if (vote.brndPaidWhenCreatingPodium) {
          // Convert to BigInt and calculate 10%
          const voteCost = BigInt(vote.brndPaidWhenCreatingPodium);
          const fee = (voteCost * BigInt(this.REPEAT_FEE_BPS)) / BigInt(10000);
          totalFees += fee;
        }
      }

      // TODO: Subtract fees already claimed
      // This would require tracking FeesClaimed events or using feeClaimNonce
      // For now, we return the total accumulated fees
      // The contract will handle preventing double claims via nonce

      return totalFees;
    } catch (error) {
      logger.error(`Error calculating fees for token ${tokenId}:`, error);
      throw new Error('Failed to calculate accumulated fees');
    }
  }
}
