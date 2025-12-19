import { Injectable } from '@nestjs/common';
import { createPublicClient, http, Address } from 'viem';
import { base } from 'viem/chains';
import { getConfig } from '../../../security/config';
import { logger } from '../../../main';

// Airdrop Contract ABI
const AIRDROP_CONTRACT_ABI = [
  {
    inputs: [
      { internalType: 'address', name: '_brndToken', type: 'address' },
      { internalType: 'address', name: '_escrowWallet', type: 'address' },
      { internalType: 'address', name: '_backendSigner', type: 'address' },
    ],
    stateMutability: 'nonpayable',
    type: 'constructor',
  },
  { inputs: [], name: 'AlreadyClaimed', type: 'error' },
  { inputs: [], name: 'ClaimWindowExpired', type: 'error' },
  { inputs: [], name: 'ClaimingDisabled', type: 'error' },
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
  { inputs: [], name: 'InvalidProof', type: 'error' },
  { inputs: [], name: 'MerkleRootAlreadySet', type: 'error' },
  { inputs: [], name: 'MerkleRootNotSet', type: 'error' },
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
      { indexed: true, internalType: 'uint256', name: 'fid', type: 'uint256' },
      {
        indexed: true,
        internalType: 'address',
        name: 'recipient',
        type: 'address',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'amountWei',
        type: 'uint256',
      },
    ],
    name: 'AirdropClaimed',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'oldSigner',
        type: 'address',
      },
      {
        indexed: true,
        internalType: 'address',
        name: 'newSigner',
        type: 'address',
      },
    ],
    name: 'BackendSignerUpdated',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      { indexed: false, internalType: 'bool', name: 'enabled', type: 'bool' },
    ],
    name: 'ClaimingEnabled',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'token',
        type: 'address',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'amount',
        type: 'uint256',
      },
    ],
    name: 'EmergencyWithdraw',
    type: 'event',
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'bytes32',
        name: 'merkleRoot',
        type: 'bytes32',
      },
      {
        indexed: true,
        internalType: 'uint256',
        name: 'startTime',
        type: 'uint256',
      },
    ],
    name: 'MerkleRootSet',
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
    inputs: [],
    name: 'BRND_TOKEN',
    outputs: [{ internalType: 'contract IBRND', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'CLAIM_WINDOW_DURATION',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
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
    name: 'airdropStartTime',
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
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'uint256', name: 'baseAmount', type: 'uint256' },
      { internalType: 'bytes32[]', name: 'proof', type: 'bytes32[]' },
    ],
    name: 'checkEligibility',
    outputs: [
      { internalType: 'bool', name: 'isEligible', type: 'bool' },
      { internalType: 'bool', name: 'hasClaimed', type: 'bool' },
      { internalType: 'bool', name: 'canClaim', type: 'bool' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'uint256', name: 'fid', type: 'uint256' },
      { internalType: 'uint256', name: 'baseAmount', type: 'uint256' },
      { internalType: 'bytes32[]', name: 'proof', type: 'bytes32[]' },
      { internalType: 'uint256', name: 'deadline', type: 'uint256' },
      { internalType: 'bytes', name: 'signature', type: 'bytes' },
    ],
    name: 'claimAirdrop',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [],
    name: 'claimingEnabled',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
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
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'fidClaimed',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'getAirdropTiming',
    outputs: [
      { internalType: 'bool', name: 'hasStarted', type: 'bool' },
      { internalType: 'uint256', name: 'startTime', type: 'uint256' },
      { internalType: 'uint256', name: 'endTime', type: 'uint256' },
      { internalType: 'uint256', name: 'timeRemaining', type: 'uint256' },
      { internalType: 'bool', name: 'isActive', type: 'bool' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'getClaimStats',
    outputs: [
      { internalType: 'uint256', name: 'totalClaimedWei', type: 'uint256' },
      { internalType: 'uint256', name: 'totalClaimers', type: 'uint256' },
      { internalType: 'uint256', name: 'totalClaimedTokens', type: 'uint256' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'getStatus',
    outputs: [
      { internalType: 'bytes32', name: 'root', type: 'bytes32' },
      { internalType: 'bool', name: 'enabled', type: 'bool' },
      {
        internalType: 'uint256',
        name: 'totalClaimedAmountWei',
        type: 'uint256',
      },
      { internalType: 'uint256', name: 'totalClaimedUsers', type: 'uint256' },
      { internalType: 'uint256', name: 'escrowBalance', type: 'uint256' },
      { internalType: 'uint256', name: 'allowance', type: 'uint256' },
      { internalType: 'uint256', name: 'startTime', type: 'uint256' },
      { internalType: 'uint256', name: 'endTime', type: 'uint256' },
      { internalType: 'bool', name: 'isActive', type: 'bool' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'fid', type: 'uint256' }],
    name: 'hasClaimed',
    outputs: [{ internalType: 'bool', name: 'claimed', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'merkleRoot',
    outputs: [{ internalType: 'bytes32', name: '', type: 'bytes32' }],
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
    inputs: [],
    name: 'renounceOwnership',
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
    inputs: [{ internalType: 'bool', name: 'enabled', type: 'bool' }],
    name: 'setClaimingEnabled',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'bytes32', name: 'newRoot', type: 'bytes32' }],
    name: 'setMerkleRoot',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function',
  },
  {
    inputs: [],
    name: 'totalClaimedWei',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'totalClaimers',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
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
export class AirdropContractService {
  private readonly contractAddress: string;
  private readonly publicClient;

  constructor() {
    const config = getConfig();
    this.contractAddress = config.blockchain.airdropContractAddress;

    if (!this.contractAddress) {
      throw new Error('AIRDROP_CONTRACT_ADDRESS not configured');
    }

    this.publicClient = createPublicClient({
      chain: base,
      transport: http(config.blockchain.baseRpcUrl),
    });

    logger.log(
      `📋 [AIRDROP CONTRACT] Service initialized with address: ${this.contractAddress}`,
    );
  }

  /**
   * Get the current status of the airdrop contract
   */
  async getContractStatus(): Promise<{
    merkleRoot: string;
    claimingEnabled: boolean;
    totalClaimed: string;
    escrowBalance: string;
    allowance: string;
  }> {
    try {
      logger.log(
        `📞 [AIRDROP CONTRACT] Calling getStatus on ${this.contractAddress}`,
      );

      const result = (await this.publicClient.readContract({
        address: this.contractAddress as Address,
        abi: AIRDROP_CONTRACT_ABI,
        functionName: 'getStatus',
      } as any)) as [string, boolean, bigint, bigint, bigint];

      const [root, enabled, totalClaimedAmount, escrowBalance, allowance] =
        result;

      // Root is already a hex string (bytes32)
      const zeroRoot =
        '0x0000000000000000000000000000000000000000000000000000000000000000';

      const status = {
        merkleRoot: root === zeroRoot ? zeroRoot : root,
        claimingEnabled: enabled,
        totalClaimed: totalClaimedAmount.toString(),
        escrowBalance: escrowBalance.toString(),
        allowance: allowance.toString(),
      };

      logger.log(`✅ [AIRDROP CONTRACT] Status retrieved successfully`, status);
      return status;
    } catch (error) {
      logger.error('Error getting airdrop contract status:', error);

      // Provide more specific error information for JSON parsing issues
      if (error.message?.includes('invalid character')) {
        logger.error(
          `💥 [AIRDROP CONTRACT] RPC JSON parsing error - possibly malformed response from RPC provider`,
        );
        throw new Error(
          'RPC provider returned malformed response. Please check RPC URL configuration.',
        );
      }

      throw error;
    }
  }

  /**
   * Check if a FID has already claimed
   */
  async hasClaimed(fid: number): Promise<boolean> {
    try {
      logger.log(`📞 [AIRDROP CONTRACT] Checking if FID ${fid} has claimed`);

      const result = (await this.publicClient.readContract({
        address: this.contractAddress as Address,
        abi: AIRDROP_CONTRACT_ABI,
        functionName: 'fidClaimed',
        args: [BigInt(fid)],
      } as any)) as boolean;

      logger.log(`✅ [AIRDROP CONTRACT] FID ${fid} claim status: ${result}`);
      return result;
    } catch (error) {
      logger.error(`Error checking if FID ${fid} has claimed:`, error);

      // Provide more specific error information for JSON parsing issues
      if (
        error.message?.includes('invalid character') ||
        error.message?.includes('JSON') ||
        error.message?.includes('Unexpected token') ||
        error.name === 'SyntaxError'
      ) {
        logger.error(
          `💥 [AIRDROP CONTRACT] RPC JSON parsing error for FID ${fid} claim check - possibly malformed response from RPC provider`,
        );
        logger.error(`RPC URL: ${getConfig().blockchain.baseRpcUrl}`);
        throw new Error(
          'RPC provider returned malformed response. Please check RPC URL configuration.',
        );
      }

      throw error;
    }
  }

  /**
   * Check if merkle root is set (not zero)
   */
  async isMerkleRootSet(): Promise<boolean> {
    try {
      const status = await this.getContractStatus();
      const zeroRoot =
        '0x0000000000000000000000000000000000000000000000000000000000000000';
      const isSet = status.merkleRoot !== zeroRoot;

      return isSet;
    } catch (error) {
      logger.error('Error checking merkle root:', error);

      // Provide more specific error information for JSON parsing issues
      if (
        error.message?.includes('invalid character') ||
        error.message?.includes('RPC provider returned malformed response')
      ) {
        logger.error(
          `💥 [AIRDROP CONTRACT] RPC JSON parsing error while checking merkle root - possibly malformed response from RPC provider`,
        );
        throw new Error(
          'RPC provider returned malformed response. Please check RPC URL configuration.',
        );
      }

      return false;
    }
  }
}
