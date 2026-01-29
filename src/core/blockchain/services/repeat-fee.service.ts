/**
 * @file RepeatFeeService - Handles fee distribution to Podium Collectible NFT owners
 * When someone votes for an arrangement that exists as a Podium Collectible,
 * the owner earns 10% of the vote cost (max 80 BRND), paid from the treasury.
 *
 * Simplified flow:
 * 1. Check if arrangement is a minted Podium Collectible (tokenId > 0)
 * 2. Get owner wallet via ownerOf(tokenId)
 * 3. Transfer BRND from treasury to owner
 */
import { Injectable, Logger } from '@nestjs/common';
import {
  createPublicClient,
  createWalletClient,
  http,
  parseUnits,
  formatUnits,
} from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { base } from 'viem/chains';

import { getConfig } from '../../../security/config';

// Podium Collectibles Contract ABI (only the functions we need)
const PODIUM_COLLECTIBLES_ABI = [
  {
    inputs: [{ internalType: 'bytes32', name: '', type: 'bytes32' }],
    name: 'arrangementToTokenId',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint16[3]', name: 'brandIds', type: 'uint16[3]' }],
    name: 'getArrangementHash',
    outputs: [{ internalType: 'bytes32', name: '', type: 'bytes32' }],
    stateMutability: 'pure',
    type: 'function',
  },
  {
    inputs: [{ internalType: 'uint256', name: 'tokenId', type: 'uint256' }],
    name: 'ownerOf',
    outputs: [{ internalType: 'address', name: '', type: 'address' }],
    stateMutability: 'view',
    type: 'function',
  },
] as const;

// ERC20 ABI (only the functions we need)
const ERC20_ABI = [
  {
    inputs: [{ internalType: 'address', name: 'account', type: 'address' }],
    name: 'balanceOf',
    outputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    stateMutability: 'view',
    type: 'function',
  },
  {
    inputs: [
      { internalType: 'address', name: 'to', type: 'address' },
      { internalType: 'uint256', name: 'amount', type: 'uint256' },
    ],
    name: 'transfer',
    outputs: [{ internalType: 'bool', name: '', type: 'bool' }],
    stateMutability: 'nonpayable',
    type: 'function',
  },
] as const;

@Injectable()
export class RepeatFeeService {
  private readonly logger = new Logger(RepeatFeeService.name);

  // Contract addresses
  private readonly PODIUM_CONTRACT_ADDRESS =
    '0x78E84851343DD61594a6588A38d1B154435B5dB2' as `0x${string}`;
  private readonly BRND_TOKEN_ADDRESS =
    '0x41Ed0311640A5e489A90940b1c33433501a21B07' as `0x${string}`;

  // Fee configuration
  private readonly FEE_PERCENTAGE = 10; // 10%
  private readonly MAX_FEE_BRND = 80; // Max 80 BRND for level 8 users

  private readonly publicClient;

  constructor() {
    const config = getConfig();
    this.publicClient = createPublicClient({
      chain: base,
      transport: http(config.blockchain.baseRpcUrl),
    });
  }

  /**
   * Get arrangement hash directly from the contract
   */
  private async getArrangementHashFromContract(
    brandIds: [number, number, number],
  ): Promise<`0x${string}`> {
    const hash = await this.publicClient.readContract({
      address: this.PODIUM_CONTRACT_ADDRESS,
      abi: PODIUM_COLLECTIBLES_ABI,
      functionName: 'getArrangementHash',
      args: [[brandIds[0], brandIds[1], brandIds[2]]],
    });
    return hash as `0x${string}`;
  }

  /**
   * Check if an arrangement exists as a Podium Collectible
   * Returns tokenId if exists, 0 if not
   */
  async getArrangementTokenId(
    brandIds: [number, number, number],
  ): Promise<bigint> {
    try {
      const arrangementHash = await this.getArrangementHashFromContract(brandIds);
      this.logger.debug(
        `🔍 Checking arrangement hash for [${brandIds.join(', ')}]: ${arrangementHash}`,
      );
      const tokenId = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_COLLECTIBLES_ABI,
        functionName: 'arrangementToTokenId',
        args: [arrangementHash],
      });
      this.logger.debug(`🔍 Contract returned tokenId: ${tokenId}`);
      return tokenId as bigint;
    } catch (error) {
      this.logger.error(
        `Error checking arrangement tokenId for [${brandIds.join(', ')}]:`,
        error,
      );
      return BigInt(0);
    }
  }

  /**
   * Get the owner wallet address for a token using ownerOf
   */
  async getOwnerWallet(tokenId: bigint): Promise<`0x${string}` | null> {
    try {
      const owner = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_COLLECTIBLES_ABI,
        functionName: 'ownerOf',
        args: [tokenId],
      });
      const ownerAddress = owner as `0x${string}`;
      if (ownerAddress === '0x0000000000000000000000000000000000000000') {
        return null;
      }
      return ownerAddress;
    } catch (error) {
      this.logger.error(`Error getting owner for token ${tokenId}:`, error);
      return null;
    }
  }

  /**
   * Calculate fee (10% of vote cost, max 80 BRND)
   */
  calculateFee(voteCostBrnd: number): number {
    const fee = voteCostBrnd * (this.FEE_PERCENTAGE / 100);
    return Math.min(fee, this.MAX_FEE_BRND);
  }

  /**
   * Get treasury (backend wallet) BRND balance
   */
  async getTreasuryBalance(): Promise<number> {
    try {
      const config = getConfig();
      const privateKey = config.blockchain.backendPrivateKey;
      if (!privateKey) {
        throw new Error('PRIVATE_KEY not set');
      }

      const formattedKey = privateKey.startsWith('0x')
        ? (privateKey as `0x${string}`)
        : (`0x${privateKey}` as `0x${string}`);

      const account = privateKeyToAccount(formattedKey);

      const balance = await this.publicClient.readContract({
        address: this.BRND_TOKEN_ADDRESS,
        abi: ERC20_ABI,
        functionName: 'balanceOf',
        args: [account.address],
      });

      return Number(formatUnits(balance as bigint, 18));
    } catch (error) {
      this.logger.error('Error getting treasury balance:', error);
      return 0;
    }
  }

  /**
   * Send BRND tokens from treasury to recipient
   */
  async sendBrndFromTreasury(
    toAddress: `0x${string}`,
    amountBrnd: number,
  ): Promise<{ success: boolean; txHash?: string; error?: string }> {
    try {
      const config = getConfig();
      const privateKey = config.blockchain.backendPrivateKey;
      if (!privateKey) {
        return { success: false, error: 'PRIVATE_KEY not set' };
      }

      const formattedKey = privateKey.startsWith('0x')
        ? (privateKey as `0x${string}`)
        : (`0x${privateKey}` as `0x${string}`);

      const account = privateKeyToAccount(formattedKey);
      const walletClient = createWalletClient({
        account,
        chain: base,
        transport: http(config.blockchain.baseRpcUrl),
      });

      const amountWei = parseUnits(amountBrnd.toString(), 18);

      const txHash = await walletClient.writeContract({
        address: this.BRND_TOKEN_ADDRESS,
        abi: ERC20_ABI,
        functionName: 'transfer',
        args: [toAddress, amountWei],
        chain: base,
        account,
      });

      this.logger.log(`📤 BRND transfer sent: ${txHash}`);

      // Wait for confirmation
      const receipt = await this.publicClient.waitForTransactionReceipt({
        hash: txHash,
      });

      if (receipt.status === 'success') {
        return { success: true, txHash };
      } else {
        return { success: false, error: 'Transaction reverted' };
      }
    } catch (error: any) {
      this.logger.error('Error sending BRND from treasury:', error);
      return { success: false, error: error?.message || 'Unknown error' };
    }
  }

  /**
   * Main method: Process fee distribution for a vote
   *
   * Simple flow:
   * 1. Check if arrangement is a Podium Collectible (tokenId > 0)
   * 2. Get owner wallet via ownerOf(tokenId)
   * 3. Calculate fee (10%, max 80 BRND)
   * 4. Transfer BRND from treasury to owner
   */
  async processVoteFeeDistribution(params: {
    voteTransactionHash: string;
    brandIds: [number, number, number];
    voterFid: number;
    voterWallet?: string;
    voteCostWei: string;
  }): Promise<{
    processed: boolean;
    reason?: string;
    txHash?: string;
  }> {
    const { voteTransactionHash, brandIds, voteCostWei } = params;

    this.logger.log(
      `🔄 Processing fee distribution for vote ${voteTransactionHash.slice(0, 10)}... [${brandIds.join(', ')}]`,
    );

    // 1. Check if arrangement exists as a Podium Collectible
    const tokenId = await this.getArrangementTokenId(brandIds);
    if (tokenId === BigInt(0)) {
      this.logger.log(
        `⏭️ Arrangement [${brandIds.join(', ')}] is not a Podium Collectible`,
      );
      return { processed: false, reason: 'Not a collectible' };
    }

    // 2. Get owner wallet via ownerOf
    const ownerWallet = await this.getOwnerWallet(tokenId);
    if (!ownerWallet) {
      this.logger.error(`❌ No owner found for token ${tokenId}`);
      return { processed: false, reason: 'No owner found' };
    }

    this.logger.log(`👤 Token ${tokenId} owner: ${ownerWallet}`);

    // 3. Calculate fee
    const voteCostBrnd = Number(formatUnits(BigInt(voteCostWei), 18));
    const feeBrnd = this.calculateFee(voteCostBrnd);

    this.logger.log(
      `💰 Vote cost: ${voteCostBrnd} BRND, Fee: ${feeBrnd} BRND (${this.FEE_PERCENTAGE}%, max ${this.MAX_FEE_BRND})`,
    );

    // 4. Check treasury balance
    const treasuryBalance = await this.getTreasuryBalance();
    if (treasuryBalance < feeBrnd) {
      this.logger.error(
        `❌ Insufficient treasury balance: ${treasuryBalance} BRND < ${feeBrnd} BRND`,
      );
      return { processed: false, reason: 'Insufficient treasury balance' };
    }

    // 5. Transfer BRND to owner
    const transferResult = await this.sendBrndFromTreasury(ownerWallet, feeBrnd);

    if (transferResult.success) {
      this.logger.log(
        `✅ Fee distributed: ${feeBrnd} BRND to ${ownerWallet} - TX: ${transferResult.txHash}`,
      );
      return { processed: true, txHash: transferResult.txHash };
    } else {
      this.logger.error(`❌ Fee transfer failed: ${transferResult.error}`);
      return { processed: false, reason: transferResult.error };
    }
  }
}
