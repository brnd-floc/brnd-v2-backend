/**
 * @file RepeatFeeService - Handles fee distribution to Podium Collectible NFT owners
 * When someone votes for an arrangement that exists as a Podium Collectible,
 * the owner earns 10% of the vote cost (max 80 BRND), paid from the treasury.
 */
import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
  createPublicClient,
  createWalletClient,
  http,
  keccak256,
  encodeAbiParameters,
  parseUnits,
  formatUnits,
} from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { base } from 'viem/chains';

import { RepeatFeeDistribution, User, Brand } from '../../../models';
import { getConfig } from '../../../security/config';
import { FarcasterNotificationService } from '../../notification/services/farcaster-notification.service';

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
    inputs: [{ internalType: 'uint256', name: '', type: 'uint256' }],
    name: 'fidWallet',
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

  // Treasury alert threshold (5 million BRND)
  private readonly TREASURY_ALERT_THRESHOLD = 5_000_000;

  // Admin FIDs for alerts
  private readonly ADMIN_FIDS = [16098, 8109, 5431];

  private readonly publicClient;

  constructor(
    @InjectRepository(RepeatFeeDistribution)
    private readonly repeatFeeRepository: Repository<RepeatFeeDistribution>,
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    private readonly notificationService: FarcasterNotificationService,
  ) {
    const config = getConfig();
    this.publicClient = createPublicClient({
      chain: base,
      transport: http(config.blockchain.baseRpcUrl),
    });
  }

  /**
   * Calculate arrangement hash from brand IDs (same as contract)
   * Order matters: [1,2,3] != [3,2,1]
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
   * Check if an arrangement exists as a Podium Collectible
   * Returns tokenId if exists, 0 if not
   */
  async getArrangementTokenId(
    brandIds: [number, number, number],
  ): Promise<bigint> {
    try {
      const arrangementHash = this.calculateArrangementHash(brandIds);
      const tokenId = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_COLLECTIBLES_ABI,
        functionName: 'arrangementToTokenId',
        args: [arrangementHash],
      });
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
   * Get podium data from contract (ownerFid, etc.)
   */
  async getPodiumData(tokenId: bigint): Promise<{
    ownerFid: bigint;
    genesisCreatorFid: bigint;
    claimCount: bigint;
  } | null> {
    try {
      const data = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_COLLECTIBLES_ABI,
        functionName: 'podiumData',
        args: [tokenId],
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
        genesisCreatorFid: result[0],
        ownerFid: result[1],
        claimCount: result[2],
      };
    } catch (error) {
      this.logger.error(`Error getting podium data for token ${tokenId}:`, error);
      return null;
    }
  }

  /**
   * Get wallet address for an FID from the contract
   */
  async getFidWallet(fid: bigint): Promise<`0x${string}` | null> {
    try {
      const wallet = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_COLLECTIBLES_ABI,
        functionName: 'fidWallet',
        args: [fid],
      });

      const walletAddress = wallet as `0x${string}`;
      // Check if it's the zero address (not set)
      if (
        walletAddress === '0x0000000000000000000000000000000000000000' ||
        !walletAddress
      ) {
        return null;
      }
      return walletAddress;
    } catch (error) {
      this.logger.error(`Error getting wallet for FID ${fid}:`, error);
      return null;
    }
  }

  /**
   * Calculate vote cost based on BRND power level
   * Level 0: 100 BRND
   * Level 1: 150 BRND
   * Level 2-8: level * 100 BRND
   */
  getVoteCost(brndPowerLevel: number): number {
    if (brndPowerLevel === 0) return 100;
    if (brndPowerLevel === 1) return 150;
    return brndPowerLevel * 100;
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

      // Convert amount to wei
      const amountWei = parseUnits(amountBrnd.toString(), 18);

      this.logger.log(
        `💸 Sending ${amountBrnd} BRND from treasury (${account.address}) to ${toAddress}`,
      );

      // Send the transfer transaction
      const txHash = await walletClient.writeContract({
        address: this.BRND_TOKEN_ADDRESS,
        abi: ERC20_ABI,
        functionName: 'transfer',
        args: [toAddress, amountWei],
        chain: base,
        account,
      });

      this.logger.log(`✅ Transfer tx sent: ${txHash}`);

      // Wait for confirmation
      const receipt = await this.publicClient.waitForTransactionReceipt({
        hash: txHash,
        confirmations: 1,
      });

      if (receipt.status === 'success') {
        this.logger.log(`✅ Transfer confirmed: ${txHash}`);
        return { success: true, txHash };
      } else {
        return { success: false, txHash, error: 'Transaction reverted' };
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Transfer failed: ${errorMessage}`);
      return { success: false, error: errorMessage };
    }
  }

  /**
   * Check if a fee distribution already exists for this vote (idempotency)
   */
  async feeDistributionExists(voteTransactionHash: string): Promise<boolean> {
    const existing = await this.repeatFeeRepository.findOne({
      where: { voteTransactionHash },
    });
    return !!existing;
  }

  /**
   * Get brand names for notification
   */
  async getBrandNames(
    brandIds: [number, number, number],
  ): Promise<[string, string, string]> {
    const brands = await this.brandRepository.findByIds(brandIds);
    const brandMap = new Map(brands.map((b) => [b.id, b.name]));
    return [
      brandMap.get(brandIds[0]) || `Brand #${brandIds[0]}`,
      brandMap.get(brandIds[1]) || `Brand #${brandIds[1]}`,
      brandMap.get(brandIds[2]) || `Brand #${brandIds[2]}`,
    ];
  }

  /**
   * Get voter username from database or Neynar
   */
  async getVoterUsername(voterFid: number): Promise<string> {
    // Try database first
    const user = await this.userRepository.findOne({
      where: { fid: voterFid },
    });
    if (user?.username) {
      return user.username;
    }

    // Fallback to Neynar
    try {
      const config = getConfig();
      const apiKey = config.neynar.apiKey?.replace(/&$/, '');
      if (!apiKey) return `FID ${voterFid}`;

      const response = await fetch(
        `https://api.neynar.com/v2/farcaster/user/bulk?fids=${voterFid}`,
        {
          headers: {
            accept: 'application/json',
            api_key: apiKey,
          },
        },
      );

      if (response.ok) {
        const data = await response.json();
        const username = data?.users?.[0]?.username;
        if (username) return username;
      }
    } catch (error) {
      this.logger.warn(`Failed to fetch username for FID ${voterFid}`);
    }

    return `FID ${voterFid}`;
  }

  /**
   * Send low treasury balance alert to all admins
   */
  async sendTreasuryLowAlert(currentBalance: number): Promise<void> {
    const formattedBalance = currentBalance.toLocaleString(undefined, {
      maximumFractionDigits: 0,
    });

    const notificationId = `treasury-low-${Date.now()}`;

    for (const adminFid of this.ADMIN_FIDS) {
      try {
        await this.notificationService.sendNotificationToSpecificFid(
          adminFid,
          'TREASURY RUNNING LOW',
          `Balance: ${formattedBalance} BRND`,
          'https://brnd.land',
          notificationId,
        );
      } catch (error) {
        this.logger.error(`Failed to send treasury alert to FID ${adminFid}:`, error);
      }
    }

    this.logger.warn(
      `⚠️ Treasury low alert sent to ${this.ADMIN_FIDS.length} admins! Balance: ${formattedBalance} BRND`,
    );
  }

  /**
   * Main method: Process fee distribution for a vote
   * Called after a vote is synced from the indexer
   *
   * CRITICAL: Uses atomic insert with duplicate key handling to ensure
   * we NEVER send tokens twice for the same vote, even under race conditions.
   */
  async processVoteFeeDistribution(params: {
    voteTransactionHash: string;
    brandIds: [number, number, number];
    voterFid: number;
    voterWallet?: string;
    voteCostWei: string; // Vote cost in wei (from contract event)
  }): Promise<{
    processed: boolean;
    reason?: string;
    distribution?: RepeatFeeDistribution;
  }> {
    const { voteTransactionHash, brandIds, voterFid, voterWallet, voteCostWei } =
      params;

    this.logger.log(
      `🔄 Processing fee distribution for vote ${voteTransactionHash.slice(0, 10)}... [${brandIds.join(', ')}]`,
    );

    // 1. Check if arrangement exists as a Podium Collectible (do this first to avoid DB operations for non-collectibles)
    const tokenId = await this.getArrangementTokenId(brandIds);
    if (tokenId === BigInt(0)) {
      this.logger.log(
        `⏭️ Arrangement [${brandIds.join(', ')}] is not a Podium Collectible`,
      );
      return { processed: false, reason: 'Not a collectible' };
    }

    // 2. Get podium data (owner FID)
    const podiumData = await this.getPodiumData(tokenId);
    if (!podiumData) {
      this.logger.error(
        `❌ Failed to get podium data for token ${tokenId}`,
      );
      return { processed: false, reason: 'Failed to get podium data' };
    }

    const ownerFid = Number(podiumData.ownerFid);
    if (ownerFid === 0) {
      this.logger.error(`❌ Owner FID is 0 for token ${tokenId}`);
      return { processed: false, reason: 'Invalid owner FID' };
    }

    // 3. Get owner's wallet address
    const ownerWallet = await this.getFidWallet(podiumData.ownerFid);
    if (!ownerWallet) {
      this.logger.error(`❌ No wallet found for owner FID ${ownerFid}`);
      return { processed: false, reason: 'Owner has no wallet' };
    }

    // 4. Calculate fee
    const voteCostBrnd = Number(formatUnits(BigInt(voteCostWei), 18));
    const feeBrnd = this.calculateFee(voteCostBrnd);
    const feeWei = parseUnits(feeBrnd.toString(), 18).toString();

    this.logger.log(
      `💰 Vote cost: ${voteCostBrnd} BRND, Fee: ${feeBrnd} BRND (${this.FEE_PERCENTAGE}%, max ${this.MAX_FEE_BRND})`,
    );

    // 5. CRITICAL: Try to create distribution record atomically
    // If another process already inserted this voteTransactionHash, we catch the
    // duplicate key error and return "Already processed" WITHOUT sending tokens
    let distribution: RepeatFeeDistribution;
    try {
      distribution = this.repeatFeeRepository.create({
        voteTransactionHash,
        tokenId: Number(tokenId),
        brand1Id: brandIds[0],
        brand2Id: brandIds[1],
        brand3Id: brandIds[2],
        voterFid,
        voterWallet: voterWallet || null,
        ownerFid,
        ownerWallet,
        feeAmountWei: feeWei,
        feeAmountBrnd: feeBrnd,
        voteCostBrnd,
        status: 'pending',
      });
      await this.repeatFeeRepository.save(distribution);
    } catch (error: any) {
      // Check if it's a duplicate key error (MySQL error code 1062)
      if (error?.code === 'ER_DUP_ENTRY' || error?.errno === 1062) {
        this.logger.log(
          `⏭️ Fee distribution already exists for vote ${voteTransactionHash.slice(0, 10)}... (caught duplicate)`,
        );
        return { processed: false, reason: 'Already processed' };
      }
      // Re-throw other errors
      throw error;
    }

    // 7. Check treasury balance
    const treasuryBalance = await this.getTreasuryBalance();
    if (treasuryBalance < feeBrnd) {
      distribution.status = 'failed';
      distribution.errorMessage = `Insufficient treasury balance: ${treasuryBalance} BRND`;
      distribution.processedAt = new Date();
      await this.repeatFeeRepository.save(distribution);

      this.logger.error(
        `❌ Insufficient treasury balance: ${treasuryBalance} < ${feeBrnd}`,
      );

      // Send alert
      await this.sendTreasuryLowAlert(treasuryBalance);

      return {
        processed: false,
        reason: 'Insufficient treasury balance',
        distribution,
      };
    }

    // Send alert if balance is low (but still proceed with transfer)
    if (treasuryBalance < this.TREASURY_ALERT_THRESHOLD) {
      await this.sendTreasuryLowAlert(treasuryBalance);
    }

    // 8. Send BRND transfer
    const transferResult = await this.sendBrndFromTreasury(ownerWallet, feeBrnd);

    if (transferResult.success) {
      distribution.status = 'success';
      distribution.transferTxHash = transferResult.txHash;
      distribution.processedAt = new Date();
      await this.repeatFeeRepository.save(distribution);

      this.logger.log(
        `✅ Fee distributed: ${feeBrnd} BRND to FID ${ownerFid} (${ownerWallet}) - TX: ${transferResult.txHash}`,
      );

      // 9. Send notification to owner
      try {
        const voterUsername = await this.getVoterUsername(voterFid);
        const brandNames = await this.getBrandNames(brandIds);

        // Format brand names for notification (truncate if too long)
        const brandList = brandNames.join(', ');
        const truncatedBrandList =
          brandList.length > 60 ? brandList.slice(0, 57) + '...' : brandList;

        // Title max 32 chars, body max 128 chars
        const title = `+${feeBrnd} $BRND earned!`;
        const body = `@${voterUsername} voted [${truncatedBrandList}]`;

        await this.notificationService.sendNotificationToSpecificFid(
          ownerFid,
          title,
          body,
          'https://brnd.land',
          `repeat-fee-${voteTransactionHash}`,
        );

        distribution.notificationSent = true;
        await this.repeatFeeRepository.save(distribution);

        this.logger.log(`📬 Notification sent to FID ${ownerFid}`);
      } catch (notifError) {
        this.logger.warn(
          `⚠️ Failed to send notification to FID ${ownerFid}:`,
          notifError,
        );
        // Don't fail the whole process for notification errors
      }

      return { processed: true, distribution };
    } else {
      distribution.status = 'failed';
      distribution.errorMessage = transferResult.error || 'Transfer failed';
      distribution.processedAt = new Date();
      await this.repeatFeeRepository.save(distribution);

      this.logger.error(
        `❌ Fee transfer failed: ${transferResult.error}`,
      );

      return {
        processed: false,
        reason: transferResult.error,
        distribution,
      };
    }
  }

  /**
   * Retry failed fee distributions
   */
  async retryFailedDistributions(): Promise<{
    attempted: number;
    succeeded: number;
  }> {
    const failedDistributions = await this.repeatFeeRepository.find({
      where: { status: 'failed' },
      order: { createdAt: 'ASC' },
      take: 10, // Process in batches
    });

    let succeeded = 0;

    for (const dist of failedDistributions) {
      this.logger.log(
        `🔄 Retrying failed distribution for vote ${dist.voteTransactionHash.slice(0, 10)}...`,
      );

      const ownerWallet = dist.ownerWallet as `0x${string}`;
      const transferResult = await this.sendBrndFromTreasury(
        ownerWallet,
        dist.feeAmountBrnd,
      );

      if (transferResult.success) {
        dist.status = 'success';
        dist.transferTxHash = transferResult.txHash;
        dist.processedAt = new Date();
        dist.errorMessage = null;
        await this.repeatFeeRepository.save(dist);
        succeeded++;

        this.logger.log(
          `✅ Retry succeeded for vote ${dist.voteTransactionHash.slice(0, 10)}`,
        );
      } else {
        dist.errorMessage = `Retry failed: ${transferResult.error}`;
        await this.repeatFeeRepository.save(dist);

        this.logger.error(
          `❌ Retry failed for vote ${dist.voteTransactionHash.slice(0, 10)}: ${transferResult.error}`,
        );
      }
    }

    return { attempted: failedDistributions.length, succeeded };
  }

  /**
   * Get distribution statistics
   */
  async getDistributionStats(): Promise<{
    total: number;
    successful: number;
    failed: number;
    pending: number;
    totalFeesDistributed: number;
  }> {
    const [total, successful, failed, pending] = await Promise.all([
      this.repeatFeeRepository.count(),
      this.repeatFeeRepository.count({ where: { status: 'success' } }),
      this.repeatFeeRepository.count({ where: { status: 'failed' } }),
      this.repeatFeeRepository.count({ where: { status: 'pending' } }),
    ]);

    // Sum of successful fees
    const result = await this.repeatFeeRepository
      .createQueryBuilder('dist')
      .select('SUM(dist.feeAmountBrnd)', 'totalFees')
      .where('dist.status = :status', { status: 'success' })
      .getRawOne();

    const totalFeesDistributed = parseFloat(result?.totalFees || '0');

    return {
      total,
      successful,
      failed,
      pending,
      totalFeesDistributed,
    };
  }
}
