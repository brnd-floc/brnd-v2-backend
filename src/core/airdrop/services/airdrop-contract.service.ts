import { Injectable } from '@nestjs/common';
import { createPublicClient, http, Address } from 'viem';
import { base } from 'viem/chains';
import { getConfig } from '../../../security/config';
import { logger } from '../../../main';

// Airdrop Contract ABI
const AIRDROP_CONTRACT_ABI = [
  {
    inputs: [],
    name: 'merkleRoot',
    outputs: [{ internalType: 'bytes32', name: '', type: 'bytes32' }],
    stateMutability: 'view',
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
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'fidClaimed',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'totalClaimed',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [],
    name: 'getStatus',
    outputs: [
      { internalType: 'bytes32', name: 'root', type: 'bytes32' },
      { internalType: 'bool', name: 'enabled', type: 'bool' },
      { internalType: 'uint256', name: 'totalClaimedAmount', type: 'uint256' },
      { internalType: 'uint256', name: 'escrowBalance', type: 'uint256' },
      { internalType: 'uint256', name: 'allowance', type: 'uint256' },
    ],
    stateMutability: 'view',
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
