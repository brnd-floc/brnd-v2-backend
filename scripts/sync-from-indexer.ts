#!/usr/bin/env bun

/**
 * Indexer Sync Script
 *
 * Syncs data from the PostgreSQL indexer database (source of truth)
 * to the MySQL production database.
 *
 * Syncs:
 * - Brands (new and updated)
 * - User brndPowerLevel
 * - Votes (missing ones)
 * - Repeat fee distributions (when votes match podium collectibles)
 *
 * Usage:
 *   bun run scripts/sync-from-indexer.ts
 *
 * Environment variables required:
 *   - INDEXER_DB_URL: PostgreSQL connection string
 *   - INDEXER_DB_SCHEMA: Schema name (default: public)
 *   - DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME: MySQL connection
 *   - BASE_RPC_URL: Base chain RPC URL
 *   - PRIVATE_KEY: Backend wallet private key (for sending BRND fees)
 */

import { Client } from 'pg';
import * as mysql from 'mysql2/promise';
import * as readline from 'readline';
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

// ============================================================================
// Types
// ============================================================================

interface SyncStats {
  brandsChecked: number;
  brandsCreated: number;
  brandsUpdated: number;
  usersChecked: number;
  usersUpdated: number;
  votesChecked: number;
  votesInserted: number;
  feesProcessed: number;
  feesSucceeded: number;
  feesFailed: number;
  errors: string[];
  startTime: Date;
  endTime?: Date;
}

interface SyncOptions {
  windowHours: number;
  syncBrands: boolean;
  syncPowerLevels: boolean;
  syncVotes: boolean;
  dryRun: boolean;
}

// ============================================================================
// Repeat Fee Processor
// ============================================================================

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

class RepeatFeeProcessor {
  private readonly PODIUM_CONTRACT_ADDRESS = '0x78E84851343DD61594a6588A38d1B154435B5dB2' as `0x${string}`;
  private readonly BRND_TOKEN_ADDRESS = '0x41Ed0311640A5e489A90940b1c33433501a21B07' as `0x${string}`;
  private readonly FEE_PERCENTAGE = 10; // 10%
  private readonly MAX_FEE_BRND = 80; // Max 80 BRND
  private readonly TREASURY_ALERT_THRESHOLD = 5_000_000; // 5M BRND
  private readonly ADMIN_FIDS = [16098, 8109, 5431];
  private readonly NOTIFICATION_URL = 'https://api.farcaster.xyz/v1/frame-notifications';

  private publicClient: any;
  private walletClient: any;
  private account: any;
  private mysqlConn: mysql.Connection;

  constructor(mysqlConn: mysql.Connection) {
    this.mysqlConn = mysqlConn;

    const rpcUrl = process.env.BASE_RPC_URL;
    if (!rpcUrl) {
      throw new Error('BASE_RPC_URL environment variable is required');
    }

    this.publicClient = createPublicClient({
      chain: base,
      transport: http(rpcUrl),
    });

    const privateKey = process.env.PRIVATE_KEY;
    if (privateKey) {
      const formattedKey = privateKey.startsWith('0x')
        ? (privateKey as `0x${string}`)
        : (`0x${privateKey}` as `0x${string}`);
      this.account = privateKeyToAccount(formattedKey);
      this.walletClient = createWalletClient({
        account: this.account,
        chain: base,
        transport: http(rpcUrl),
      });
    }
  }

  private calculateArrangementHash(brandIds: [number, number, number]): `0x${string}` {
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

  async getArrangementTokenId(brandIds: [number, number, number]): Promise<bigint> {
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
      return BigInt(0);
    }
  }

  async getPodiumData(tokenId: bigint): Promise<{ ownerFid: bigint } | null> {
    try {
      const data = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_COLLECTIBLES_ABI,
        functionName: 'podiumData',
        args: [tokenId],
      });
      const result = data as readonly [bigint, bigint, bigint, bigint, bigint, bigint];
      return { ownerFid: result[1] };
    } catch (error) {
      return null;
    }
  }

  async getFidWallet(fid: bigint): Promise<`0x${string}` | null> {
    try {
      const wallet = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_COLLECTIBLES_ABI,
        functionName: 'fidWallet',
        args: [fid],
      });
      const walletAddress = wallet as `0x${string}`;
      if (walletAddress === '0x0000000000000000000000000000000000000000' || !walletAddress) {
        return null;
      }
      return walletAddress;
    } catch (error) {
      return null;
    }
  }

  calculateFee(voteCostBrnd: number): number {
    const fee = voteCostBrnd * (this.FEE_PERCENTAGE / 100);
    return Math.min(fee, this.MAX_FEE_BRND);
  }

  async getTreasuryBalance(): Promise<number> {
    if (!this.account) return 0;
    try {
      const balance = await this.publicClient.readContract({
        address: this.BRND_TOKEN_ADDRESS,
        abi: ERC20_ABI,
        functionName: 'balanceOf',
        args: [this.account.address],
      });
      return Number(formatUnits(balance as bigint, 18));
    } catch (error) {
      return 0;
    }
  }

  async sendBrndFromTreasury(
    toAddress: `0x${string}`,
    amountBrnd: number,
  ): Promise<{ success: boolean; txHash?: string; error?: string }> {
    if (!this.walletClient || !this.account) {
      return { success: false, error: 'Wallet not configured' };
    }

    try {
      const amountWei = parseUnits(amountBrnd.toString(), 18);

      const txHash = await this.walletClient.writeContract({
        address: this.BRND_TOKEN_ADDRESS,
        abi: ERC20_ABI,
        functionName: 'transfer',
        args: [toAddress, amountWei],
      });

      // Wait for confirmation
      const receipt = await this.publicClient.waitForTransactionReceipt({
        hash: txHash,
        confirmations: 1,
      });

      if (receipt.status === 'success') {
        return { success: true, txHash };
      } else {
        return { success: false, txHash, error: 'Transaction reverted' };
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      return { success: false, error: errorMessage };
    }
  }

  async feeDistributionExists(voteTransactionHash: string): Promise<boolean> {
    const [rows] = await this.mysqlConn.execute(
      'SELECT id FROM repeat_fee_distributions WHERE voteTransactionHash = ?',
      [voteTransactionHash],
    );
    return (rows as any[]).length > 0;
  }

  async getBrandNames(brandIds: [number, number, number]): Promise<[string, string, string]> {
    const [rows] = await this.mysqlConn.execute(
      'SELECT id, name FROM brands WHERE id IN (?, ?, ?)',
      brandIds,
    );
    const brandMap = new Map((rows as any[]).map((b) => [b.id, b.name]));
    return [
      brandMap.get(brandIds[0]) || `Brand #${brandIds[0]}`,
      brandMap.get(brandIds[1]) || `Brand #${brandIds[1]}`,
      brandMap.get(brandIds[2]) || `Brand #${brandIds[2]}`,
    ];
  }

  async getVoterUsername(voterFid: number): Promise<string> {
    const [rows] = await this.mysqlConn.execute(
      'SELECT username FROM users WHERE fid = ?',
      [voterFid],
    );
    const users = rows as any[];
    if (users.length > 0 && users[0].username) {
      return users[0].username;
    }
    return `FID ${voterFid}`;
  }

  async getOwnerNotificationToken(ownerFid: number): Promise<string | null> {
    const [rows] = await this.mysqlConn.execute(
      'SELECT notificationToken FROM users WHERE fid = ? AND notificationsEnabled = true AND notificationToken IS NOT NULL',
      [ownerFid],
    );
    const users = rows as any[];
    return users.length > 0 ? users[0].notificationToken : null;
  }

  async sendNotification(
    ownerFid: number,
    title: string,
    body: string,
    notificationId: string,
  ): Promise<boolean> {
    try {
      const notificationToken = await this.getOwnerNotificationToken(ownerFid);
      if (!notificationToken) {
        return false;
      }

      const response = await fetch(this.NOTIFICATION_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          notificationId,
          title,
          body,
          targetUrl: 'https://brnd.land',
          tokens: [notificationToken],
        }),
      });

      return response.ok;
    } catch (error) {
      return false;
    }
  }

  async sendTreasuryLowAlert(currentBalance: number): Promise<void> {
    const formattedBalance = currentBalance.toLocaleString(undefined, { maximumFractionDigits: 0 });
    const notificationId = `treasury-low-${Date.now()}`;

    for (const adminFid of this.ADMIN_FIDS) {
      await this.sendNotification(
        adminFid,
        'TREASURY RUNNING LOW',
        `Balance: ${formattedBalance} BRND`,
        notificationId,
      );
    }
    console.log(`      ⚠️ Treasury low alert sent to ${this.ADMIN_FIDS.length} admins! Balance: ${formattedBalance} BRND`);
  }

  async processVoteFee(params: {
    voteTransactionHash: string;
    brandIds: [number, number, number];
    voterFid: number;
    voterWallet?: string;
    voteCostWei: string;
    dryRun: boolean;
  }): Promise<{ processed: boolean; reason?: string }> {
    const { voteTransactionHash, brandIds, voterFid, voterWallet, voteCostWei, dryRun } = params;

    // 1. Check if arrangement exists as a Podium Collectible (do this first to avoid unnecessary DB checks)
    const tokenId = await this.getArrangementTokenId(brandIds);
    if (tokenId === BigInt(0)) {
      return { processed: false, reason: 'Not a collectible' };
    }

    // 2. Get podium data (owner FID)
    const podiumData = await this.getPodiumData(tokenId);
    if (!podiumData) {
      return { processed: false, reason: 'Failed to get podium data' };
    }

    const ownerFid = Number(podiumData.ownerFid);
    if (ownerFid === 0) {
      return { processed: false, reason: 'Invalid owner FID' };
    }

    // 3. Get owner's wallet address
    const ownerWallet = await this.getFidWallet(podiumData.ownerFid);
    if (!ownerWallet) {
      return { processed: false, reason: 'Owner has no wallet' };
    }

    // 4. Calculate fee
    const voteCostBrnd = Number(formatUnits(BigInt(voteCostWei), 18));
    const feeBrnd = this.calculateFee(voteCostBrnd);
    const feeWei = parseUnits(feeBrnd.toString(), 18).toString();

    if (dryRun) {
      console.log(`      [DRY RUN] Would distribute ${feeBrnd} BRND to FID ${ownerFid} for collectible #${tokenId}`);
      return { processed: true };
    }

    // 5. CRITICAL: Use INSERT IGNORE to atomically check-and-insert
    // This ensures only ONE process can ever create a record for this vote
    // If another process already inserted, affectedRows will be 0
    const [insertResult] = await this.mysqlConn.execute(
      `INSERT IGNORE INTO repeat_fee_distributions
       (voteTransactionHash, tokenId, brand1Id, brand2Id, brand3Id, voterFid, voterWallet,
        ownerFid, ownerWallet, feeAmountWei, feeAmountBrnd, voteCostBrnd, status, createdAt)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NOW())`,
      [
        voteTransactionHash,
        Number(tokenId),
        brandIds[0],
        brandIds[1],
        brandIds[2],
        voterFid,
        voterWallet || null,
        ownerFid,
        ownerWallet,
        feeWei,
        feeBrnd,
        voteCostBrnd,
      ],
    );

    // If affectedRows is 0, another process already claimed this vote - DO NOT send tokens
    if ((insertResult as any).affectedRows === 0) {
      return { processed: false, reason: 'Already processed' };
    }

    // 7. Check treasury balance
    const treasuryBalance = await this.getTreasuryBalance();
    if (treasuryBalance < feeBrnd) {
      await this.mysqlConn.execute(
        `UPDATE repeat_fee_distributions SET status = 'failed', errorMessage = ?, processedAt = NOW()
         WHERE voteTransactionHash = ?`,
        [`Insufficient treasury balance: ${treasuryBalance} BRND`, voteTransactionHash],
      );
      await this.sendTreasuryLowAlert(treasuryBalance);
      return { processed: false, reason: 'Insufficient treasury balance' };
    }

    // Send alert if balance is low
    if (treasuryBalance < this.TREASURY_ALERT_THRESHOLD) {
      await this.sendTreasuryLowAlert(treasuryBalance);
    }

    // 8. Send BRND transfer
    const transferResult = await this.sendBrndFromTreasury(ownerWallet, feeBrnd);

    if (transferResult.success) {
      await this.mysqlConn.execute(
        `UPDATE repeat_fee_distributions SET status = 'success', transferTxHash = ?, processedAt = NOW()
         WHERE voteTransactionHash = ?`,
        [transferResult.txHash, voteTransactionHash],
      );

      console.log(`      ✅ Fee: ${feeBrnd} BRND -> FID ${ownerFid} (TX: ${transferResult.txHash?.slice(0, 10)}...)`);

      // 9. Send notification
      try {
        const voterUsername = await this.getVoterUsername(voterFid);
        const brandNames = await this.getBrandNames(brandIds);
        const brandList = brandNames.join(', ');
        const truncatedBrandList = brandList.length > 60 ? brandList.slice(0, 57) + '...' : brandList;

        const title = `+${feeBrnd} $BRND earned!`;
        const body = `@${voterUsername} voted [${truncatedBrandList}]`;

        const notifSent = await this.sendNotification(ownerFid, title, body, `repeat-fee-${voteTransactionHash}`);
        if (notifSent) {
          await this.mysqlConn.execute(
            'UPDATE repeat_fee_distributions SET notificationSent = true WHERE voteTransactionHash = ?',
            [voteTransactionHash],
          );
        }
      } catch (error) {
        // Don't fail for notification errors
      }

      return { processed: true };
    } else {
      await this.mysqlConn.execute(
        `UPDATE repeat_fee_distributions SET status = 'failed', errorMessage = ?, processedAt = NOW()
         WHERE voteTransactionHash = ?`,
        [transferResult.error || 'Transfer failed', voteTransactionHash],
      );
      return { processed: false, reason: transferResult.error };
    }
  }
}

// ============================================================================
// Prompts
// ============================================================================

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

function prompt(question: string): Promise<string> {
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      resolve(answer);
    });
  });
}

async function promptForOptions(): Promise<SyncOptions> {
  console.log('\n🔄 INDEXER SYNC SCRIPT');
  console.log('='.repeat(50));
  console.log('\nThis script syncs data from the PostgreSQL indexer');
  console.log('to the MySQL production database.\n');

  // Time window selection
  console.log('Select sync time window:');
  console.log('  1. Last 48 hours (recommended for daily sync)');
  console.log('  2. Last 7 days');
  console.log('  3. Last 30 days');
  console.log('  4. Full sync (all data)');
  console.log('  5. Custom (enter hours)');
  console.log('');

  const windowChoice = await prompt('Enter choice (1-5): ');

  let windowHours: number;
  switch (windowChoice.trim()) {
    case '1':
      windowHours = 48;
      break;
    case '2':
      windowHours = 168; // 7 * 24
      break;
    case '3':
      windowHours = 720; // 30 * 24
      break;
    case '4':
      windowHours = 0; // Full sync
      break;
    case '5':
      const customHours = await prompt('Enter number of hours: ');
      windowHours = parseInt(customHours, 10) || 48;
      break;
    default:
      console.log('Invalid choice, defaulting to 48 hours');
      windowHours = 48;
  }

  // What to sync
  console.log('\nWhat do you want to sync?');
  console.log('  1. All (brands, power levels, votes) (recommended)');
  console.log('  2. Only brands');
  console.log('  3. Only power levels');
  console.log('  4. Only votes');
  console.log('  5. Brands and votes (skip power levels)');
  console.log('');

  const syncChoice = await prompt('Enter choice (1-5): ');

  let syncBrands = true;
  let syncPowerLevels = true;
  let syncVotes = true;

  switch (syncChoice.trim()) {
    case '2':
      syncPowerLevels = false;
      syncVotes = false;
      break;
    case '3':
      syncBrands = false;
      syncVotes = false;
      break;
    case '4':
      syncBrands = false;
      syncPowerLevels = false;
      break;
    case '5':
      syncPowerLevels = false;
      break;
    default:
      // Default to all
      break;
  }

  // Dry run option
  console.log('\nRun mode:');
  console.log('  1. Live (actually sync data)');
  console.log('  2. Dry run (preview only, no changes)');
  console.log('');

  const modeChoice = await prompt('Enter choice (1-2): ');
  const dryRun = modeChoice.trim() === '2';

  // Confirm
  console.log('\n' + '='.repeat(50));
  console.log('SYNC CONFIGURATION:');
  console.log(`  Window: ${windowHours === 0 ? 'FULL SYNC' : `Last ${windowHours} hours`}`);
  console.log(`  Sync brands: ${syncBrands ? 'Yes' : 'No'}`);
  console.log(`  Sync power levels: ${syncPowerLevels ? 'Yes' : 'No'}`);
  console.log(`  Sync votes: ${syncVotes ? 'Yes' : 'No'}`);
  console.log(`  Mode: ${dryRun ? '🔍 DRY RUN (no changes)' : '🚀 LIVE'}`);
  console.log('='.repeat(50));

  const confirm = await prompt('\nProceed with sync? (y/n): ');
  if (confirm.toLowerCase() !== 'y') {
    console.log('Sync cancelled.');
    process.exit(0);
  }

  return { windowHours, syncBrands, syncPowerLevels, syncVotes, dryRun };
}

// ============================================================================
// Sync Logic
// ============================================================================

class IndexerSyncer {
  private pgClient: Client;
  private mysqlConn: mysql.Connection | null = null;
  private feeProcessor: RepeatFeeProcessor | null = null;
  private schema: string;
  private stats: SyncStats;

  constructor() {
    const connectionString = process.env.INDEXER_DB_URL;
    if (!connectionString) {
      throw new Error('INDEXER_DB_URL environment variable is required');
    }

    this.pgClient = new Client({ connectionString });
    this.schema = process.env.INDEXER_DB_SCHEMA || 'public';
    this.stats = {
      brandsChecked: 0,
      brandsCreated: 0,
      brandsUpdated: 0,
      usersChecked: 0,
      usersUpdated: 0,
      votesChecked: 0,
      votesInserted: 0,
      feesProcessed: 0,
      feesSucceeded: 0,
      feesFailed: 0,
      errors: [],
      startTime: new Date(),
    };
  }

  async connect(): Promise<void> {
    console.log('\n🔌 Connecting to databases...');

    // Connect to PostgreSQL
    await this.pgClient.connect();

    // Connect to MySQL
    const mysqlConfig = {
      host: process.env.DATABASE_HOST,
      port: parseInt(process.env.DATABASE_PORT || '3306', 10),
      user: process.env.DATABASE_USER,
      password: process.env.DATABASE_PASSWORD,
      database: process.env.DATABASE_NAME,
    };

    if (!mysqlConfig.host || !mysqlConfig.user || !mysqlConfig.database) {
      throw new Error(
        'MySQL environment variables required: DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME',
      );
    }

    this.mysqlConn = await mysql.createConnection(mysqlConfig);

    // Initialize fee processor
    try {
      this.feeProcessor = new RepeatFeeProcessor(this.mysqlConn);
      console.log('✅ Repeat fee processor initialized');
    } catch (error) {
      console.log('⚠️ Repeat fee processor not initialized (PRIVATE_KEY or BASE_RPC_URL may be missing)');
      this.feeProcessor = null;
    }
  }

  async disconnect(): Promise<void> {
    console.log('\n🔌 Closing database connections...');
    await this.pgClient.end();
    if (this.mysqlConn) {
      await this.mysqlConn.end();
    }
    console.log('✅ Connections closed');
  }

  async sync(options: SyncOptions): Promise<SyncStats> {
    this.stats.startTime = new Date();

    if (options.dryRun) {
      console.log('\n🔍 DRY RUN MODE - No changes will be made\n');
    }

    try {
      // Sync brands FIRST so votes have valid brand references
      if (options.syncBrands) {
        await this.syncBrands(options.windowHours, options.dryRun);
      }

      if (options.syncPowerLevels) {
        await this.syncPowerLevels(options.windowHours, options.dryRun);
      }

      if (options.syncVotes) {
        await this.syncVotes(options.windowHours, options.dryRun);
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.stats.errors.push(`Fatal: ${msg}`);
      console.error('❌ Sync error:', error);
    }

    this.stats.endTime = new Date();
    return this.stats;
  }

  private async syncPowerLevels(windowHours: number, dryRun: boolean = false): Promise<void> {
    console.log(`\n📊 Syncing user power levels...${dryRun ? ' [DRY RUN]' : ''}`);

    const isFullSync = windowHours === 0;
    let query: string;

    if (isFullSync) {
      query = `
        SELECT fid, brnd_power_level
        FROM "${this.schema}".users
        WHERE brnd_power_level > 0
      `;
    } else {
      const windowStart = Math.floor(
        (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
      );
      query = `
        SELECT DISTINCT u.fid, u.brnd_power_level
        FROM "${this.schema}".users u
        INNER JOIN "${this.schema}".brnd_power_level_ups lup ON u.fid = lup.fid
        WHERE lup.timestamp >= ${windowStart}
      `;
    }

    const result = await this.pgClient.query(query);
    const indexerUsers = result.rows;

    console.log(`   Found ${indexerUsers.length} users to check`);
    this.stats.usersChecked = indexerUsers.length;

    if (indexerUsers.length === 0) {
      console.log('   No users to sync');
      return;
    }

    // Get FIDs
    const fids = indexerUsers.map((u: any) => u.fid);

    // Fetch MySQL users
    const [mysqlRows] = await this.mysqlConn!.execute(
      `SELECT id, fid, brndPowerLevel FROM users WHERE fid IN (${fids.join(',')})`,
    );
    const mysqlUsers = mysqlRows as any[];
    const mysqlUserMap = new Map(mysqlUsers.map((u) => [u.fid, u]));

    // Process each user
    for (const indexerUser of indexerUsers) {
      const fid = indexerUser.fid;
      const indexerLevel = indexerUser.brnd_power_level;
      const mysqlUser = mysqlUserMap.get(fid);

      if (!mysqlUser) {
        console.log(`   ⚠️ User FID ${fid} not in MySQL, skipping`);
        continue;
      }

      // Only update if indexer level is HIGHER (power levels only go up)
      if (indexerLevel > mysqlUser.brndPowerLevel) {
        console.log(
          `   ${dryRun ? '[DRY RUN] Would update' : 'Updating'} FID ${fid}: level ${mysqlUser.brndPowerLevel} -> ${indexerLevel}`,
        );

        if (!dryRun) {
          await this.mysqlConn!.execute(
            'UPDATE users SET brndPowerLevel = ? WHERE fid = ?',
            [indexerLevel, fid],
          );
        }
        this.stats.usersUpdated++;
      }
    }

    console.log(`   ✅ ${dryRun ? 'Would update' : 'Updated'} ${this.stats.usersUpdated} users`);
  }

  private async syncVotes(windowHours: number, dryRun: boolean = false): Promise<void> {
    console.log(`\n📊 Syncing votes...${dryRun ? ' [DRY RUN]' : ''}`);

    const isFullSync = windowHours === 0;
    let query: string;

    if (isFullSync) {
      query = `
        SELECT id, voter, fid, day, brand_ids, cost, block_number, transaction_hash, timestamp
        FROM "${this.schema}".votes
        ORDER BY timestamp ASC
      `;
    } else {
      const windowStart = Math.floor(
        (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
      );
      query = `
        SELECT id, voter, fid, day, brand_ids, cost, block_number, transaction_hash, timestamp
        FROM "${this.schema}".votes
        WHERE timestamp >= ${windowStart}
        ORDER BY timestamp ASC
      `;
    }

    const result = await this.pgClient.query(query);
    const indexerVotes = result.rows;

    console.log(`   Found ${indexerVotes.length} votes to check`);
    this.stats.votesChecked = indexerVotes.length;

    if (indexerVotes.length === 0) {
      console.log('   No votes to sync');
      return;
    }

    // Get existing votes from MySQL
    const txHashes = indexerVotes.map((v: any) => `'${v.transaction_hash}'`);

    // Process in chunks to avoid query too large
    const CHUNK_SIZE = 500;
    const existingTxSet = new Set<string>();

    for (let i = 0; i < txHashes.length; i += CHUNK_SIZE) {
      const chunk = txHashes.slice(i, i + CHUNK_SIZE);
      const [rows] = await this.mysqlConn!.execute(
        `SELECT transactionHash FROM user_brand_votes WHERE transactionHash IN (${chunk.join(',')})`,
      );
      (rows as any[]).forEach((r) => existingTxSet.add(r.transactionHash));
    }

    console.log(`   ${existingTxSet.size} votes already exist in MySQL`);

    // Preload user cache
    const fids = [...new Set(indexerVotes.map((v: any) => v.fid))];
    const [userRows] = await this.mysqlConn!.execute(
      `SELECT id, fid FROM users WHERE fid IN (${fids.join(',')})`,
    );
    const userFidToId = new Map((userRows as any[]).map((u) => [u.fid, u.id]));

    // Preload brand cache
    const [brandRows] = await this.mysqlConn!.execute('SELECT id FROM brands');
    const brandIds = new Set((brandRows as any[]).map((b) => b.id));

    // Process votes
    const BATCH_SIZE = 50;
    for (let i = 0; i < indexerVotes.length; i += BATCH_SIZE) {
      const batch = indexerVotes.slice(i, i + BATCH_SIZE);

      for (const vote of batch) {
        const txHash = vote.transaction_hash;

        if (existingTxSet.has(txHash)) {
          continue; // Already exists
        }

        try {
          // Parse brand IDs
          let brandIdsArray: number[];
          try {
            brandIdsArray = JSON.parse(vote.brand_ids);
          } catch {
            continue;
          }

          if (brandIdsArray.length !== 3) continue;

          // Check brands exist
          if (!brandIdsArray.every((id) => brandIds.has(id))) {
            continue;
          }

          // Get user ID
          let userId = userFidToId.get(vote.fid);
          if (!userId) {
            if (dryRun) {
              // In dry run, use a placeholder
              userId = -1;
              console.log(`   [DRY RUN] Would create user for FID ${vote.fid}`);
            } else {
              // Create minimal user
              const [insertResult] = await this.mysqlConn!.execute(
                `INSERT INTO users (fid, username, photoUrl, address, points, dailyStreak, maxDailyStreak,
                 totalPodiums, votedBrandsCount, brndPowerLevel, totalVotes, banned, powerups, verified,
                 notificationsEnabled, neynarScore, createdAt, updatedAt)
                 VALUES (?, ?, '', ?, 0, 0, 0, 0, 0, 0, 0, false, 0, false, false, 0, NOW(), NOW())`,
                [vote.fid, `user_${vote.fid}`, vote.voter],
              );
              userId = (insertResult as any).insertId;
              userFidToId.set(vote.fid, userId);
            }
          }

          // Calculate vote data
          const voteDate = new Date(Number(vote.timestamp) * 1000);
          const day = Math.floor(Number(vote.timestamp) / 86400);
          const costBigInt = BigInt(vote.cost);
          const brndPaid = Number(costBigInt / BigInt(10 ** 18));
          const rewardAmount = (costBigInt * 10n).toString();

          // Insert vote
          if (!dryRun) {
            await this.mysqlConn!.execute(
              `INSERT INTO user_brand_votes
               (transactionHash, id, userId, brand1Id, brand2Id, brand3Id, date, day,
                brndPaidWhenCreatingPodium, rewardAmount, shared, shareVerified, pointsEarned)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false, false, 3)`,
              [
                txHash,
                vote.id,
                userId,
                brandIdsArray[0],
                brandIdsArray[1],
                brandIdsArray[2],
                voteDate,
                day,
                brndPaid,
                rewardAmount,
              ],
            );
          }

          this.stats.votesInserted++;
          existingTxSet.add(txHash);

          // Update brand scores (60%/30%/10% of BRND paid for 1st/2nd/3rd place)
          if (!dryRun && brndPaid > 0) {
            const score1 = Math.round(0.6 * brndPaid);
            const score2 = Math.round(0.3 * brndPaid);
            const score3 = Math.round(0.1 * brndPaid);

            await this.mysqlConn!.execute(
              `UPDATE brands SET score = score + ? WHERE id = ?`,
              [score1, brandIdsArray[0]],
            );
            await this.mysqlConn!.execute(
              `UPDATE brands SET score = score + ? WHERE id = ?`,
              [score2, brandIdsArray[1]],
            );
            await this.mysqlConn!.execute(
              `UPDATE brands SET score = score + ? WHERE id = ?`,
              [score3, brandIdsArray[2]],
            );
          }

          // Process repeat fee distribution if this vote matches a collectible
          if (this.feeProcessor) {
            try {
              const feeResult = await this.feeProcessor.processVoteFee({
                voteTransactionHash: txHash,
                brandIds: [brandIdsArray[0], brandIdsArray[1], brandIdsArray[2]] as [number, number, number],
                voterFid: vote.fid,
                voterWallet: vote.voter,
                voteCostWei: vote.cost,
                dryRun,
              });

              if (feeResult.processed) {
                this.stats.feesProcessed++;
                this.stats.feesSucceeded++;
              } else if (feeResult.reason && feeResult.reason !== 'Not a collectible' && feeResult.reason !== 'Already processed') {
                this.stats.feesProcessed++;
                this.stats.feesFailed++;
              }
            } catch (feeError) {
              const feeMsg = feeError instanceof Error ? feeError.message : String(feeError);
              this.stats.errors.push(`Fee processing for ${txHash}: ${feeMsg}`);
            }
          }
        } catch (error) {
          const msg = error instanceof Error ? error.message : String(error);
          this.stats.errors.push(`Vote ${txHash}: ${msg}`);
        }
      }

      // Progress log
      const progress = Math.min(i + BATCH_SIZE, indexerVotes.length);
      process.stdout.write(
        `\r   Processing: ${progress}/${indexerVotes.length} (${this.stats.votesInserted} inserted, ${this.stats.feesSucceeded} fees)`,
      );
    }

    console.log(`\n   ✅ ${dryRun ? 'Would insert' : 'Inserted'} ${this.stats.votesInserted} votes`);
    if (this.stats.feesProcessed > 0 || this.stats.feesSucceeded > 0) {
      console.log(`   💰 Fees: ${this.stats.feesSucceeded} succeeded, ${this.stats.feesFailed} failed`);
    }
  }

  private async syncBrands(windowHours: number, dryRun: boolean = false): Promise<void> {
    console.log(`\n📊 Syncing brands...${dryRun ? ' [DRY RUN]' : ''}`);

    const isFullSync = windowHours === 0;
    let query: string;

    if (isFullSync) {
      query = `
        SELECT id, fid, wallet_address, handle, metadata_hash, total_brnd_awarded,
               available_brnd, created_at, block_number, transaction_hash
        FROM "${this.schema}".brands
        ORDER BY id ASC
      `;
    } else {
      const windowStart = Math.floor(
        (Date.now() - windowHours * 60 * 60 * 1000) / 1000,
      );
      query = `
        SELECT id, fid, wallet_address, handle, metadata_hash, total_brnd_awarded,
               available_brnd, created_at, block_number, transaction_hash
        FROM "${this.schema}".brands
        WHERE created_at >= ${windowStart}
        ORDER BY id ASC
      `;
    }

    const result = await this.pgClient.query(query);
    const indexerBrands = result.rows;

    console.log(`   Found ${indexerBrands.length} brands to check`);
    this.stats.brandsChecked = indexerBrands.length;

    if (indexerBrands.length === 0) {
      console.log('   No brands to sync');
      return;
    }

    // Get existing brands from MySQL by onChainId
    const onChainIds = indexerBrands.map((b: any) => b.id);
    const [existingRows] = await this.mysqlConn!.execute(
      `SELECT id, onChainId, metadataHash FROM brands WHERE onChainId IN (${onChainIds.join(',')})`,
    );
    const existingBrandMap = new Map(
      (existingRows as any[]).map((b) => [b.onChainId, b]),
    );

    console.log(`   ${existingBrandMap.size} brands already exist in MySQL`);

    // Get or create default category
    let [categoryRows] = await this.mysqlConn!.execute(
      `SELECT id FROM category WHERE name = 'General' LIMIT 1`,
    );
    let defaultCategoryId: number;
    if ((categoryRows as any[]).length === 0) {
      const [insertResult] = await this.mysqlConn!.execute(
        `INSERT INTO category (name) VALUES ('General')`,
      );
      defaultCategoryId = (insertResult as any).insertId;
    } else {
      defaultCategoryId = (categoryRows as any[])[0].id;
    }

    // Process each brand
    for (const indexerBrand of indexerBrands) {
      try {
        const onChainId = indexerBrand.id;
        const existingBrand = existingBrandMap.get(onChainId);

        // Normalize metadataHash (strip ipfs:// prefix if present)
        let metadataHash = indexerBrand.metadata_hash || '';
        if (metadataHash.startsWith('ipfs://')) {
          metadataHash = metadataHash.slice(7);
        } else if (metadataHash.startsWith('/ipfs/')) {
          metadataHash = metadataHash.slice(6);
        }

        if (existingBrand) {
          // Brand exists - check if metadata hash changed
          if (existingBrand.metadataHash !== metadataHash && metadataHash) {
            console.log(`   ${dryRun ? '[DRY RUN] Would update' : 'Updating'} brand onChainId ${onChainId}: metadata changed`);

            // Fetch metadata from IPFS
            const metadata = await this.fetchIpfsMetadata(metadataHash);

            // Update the brand
            if (!dryRun) {
              await this.mysqlConn!.execute(
                `UPDATE brands SET
                  metadataHash = ?,
                  onChainHandle = ?,
                  onChainFid = ?,
                  onChainWalletAddress = ?,
                  totalBrndAwarded = ?,
                  availableBrnd = ?,
                  name = COALESCE(?, name),
                  description = COALESCE(?, description),
                  imageUrl = COALESCE(?, imageUrl),
                  url = COALESCE(?, url),
                  profile = COALESCE(?, profile),
                  channel = COALESCE(?, channel),
                  updatedAt = NOW()
                WHERE onChainId = ?`,
                [
                  metadataHash,
                  indexerBrand.handle,
                  indexerBrand.fid,
                  indexerBrand.wallet_address,
                  indexerBrand.total_brnd_awarded?.toString() || '0',
                  indexerBrand.available_brnd?.toString() || '0',
                  metadata.name || null,
                  metadata.description || null,
                  metadata.imageUrl || null,
                  metadata.url || null,
                  metadata.profile || null,
                  metadata.channel || null,
                  onChainId,
                ],
              );
            }
            this.stats.brandsUpdated++;
          }
          continue;
        }

        // Brand doesn't exist - create it
        console.log(`   ${dryRun ? '[DRY RUN] Would create' : 'Creating'} brand onChainId ${onChainId}: ${indexerBrand.handle}`);

        // Fetch metadata from IPFS
        const metadata = await this.fetchIpfsMetadata(metadataHash);

        // Determine profile/channel
        let profile = metadata.profile || '';
        let channel = metadata.channel || '';
        let queryType = metadata.queryType ?? 0;

        if (!profile && !channel) {
          channel = `/${indexerBrand.handle}`;
          queryType = 0;
        }

        // Insert the brand
        if (!dryRun) {
          await this.mysqlConn!.execute(
            `INSERT INTO brands (
              onChainId, onChainHandle, onChainFid, onChainWalletAddress, onChainCreatedAt,
              metadataHash, name, url, warpcastUrl, description, imageUrl, profile, channel,
              queryType, followerCount, categoryId, score, stateScore, scoreDay, stateScoreDay,
              scoreWeek, stateScoreWeek, scoreMonth, stateScoreMonth, ranking, rankingWeek,
              rankingMonth, bonusPoints, banned, currentRanking, totalBrndAwarded, availableBrnd,
              createdAt, updatedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
            [
              indexerBrand.id,
              indexerBrand.handle,
              indexerBrand.fid,
              indexerBrand.wallet_address,
              new Date(Number(indexerBrand.created_at) * 1000),
              metadataHash,
              metadata.name || indexerBrand.handle,
              metadata.url || '',
              metadata.warpcastUrl || metadata.url || '',
              metadata.description || '',
              metadata.imageUrl || '',
              profile,
              channel,
              queryType,
              metadata.followerCount || 0,
              defaultCategoryId,
              0, // score
              0, // stateScore
              0, // scoreDay
              0, // stateScoreDay
              0, // scoreWeek
              0, // stateScoreWeek
              0, // scoreMonth
              0, // stateScoreMonth
              '0', // ranking
              0, // rankingWeek
              0, // rankingMonth
              0, // bonusPoints
              0, // banned
              0, // currentRanking
              indexerBrand.total_brnd_awarded?.toString() || '0',
              indexerBrand.available_brnd?.toString() || '0',
            ],
          );
          console.log(`   ✅ Created brand: ${metadata.name || indexerBrand.handle} (onChainId: ${onChainId})`);
        }

        this.stats.brandsCreated++;
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        this.stats.errors.push(`Brand ${indexerBrand.id} (${indexerBrand.handle}): ${msg}`);
        console.error(`   ❌ Error syncing brand ${indexerBrand.id}: ${msg}`);
      }
    }

    console.log(`   ✅ Brands sync complete: ${this.stats.brandsCreated} ${dryRun ? 'would be' : ''} created, ${this.stats.brandsUpdated} ${dryRun ? 'would be' : ''} updated`);
  }

  private async fetchIpfsMetadata(metadataHash: string): Promise<any> {
    if (!metadataHash) return {};

    const gateways = [
      `https://ipfs.io/ipfs/${metadataHash}`,
      `https://cloudflare-ipfs.com/ipfs/${metadataHash}`,
      `https://gateway.pinata.cloud/ipfs/${metadataHash}`,
    ];

    for (const gateway of gateways) {
      try {
        const response = await fetch(gateway);
        if (response.ok) {
          return await response.json();
        }
      } catch {
        // Try next gateway
      }
    }

    console.log(`   ⚠️ Failed to fetch IPFS metadata: ${metadataHash}`);
    return {};
  }

  printSummary(): void {
    const duration = this.stats.endTime
      ? Math.round(
          (this.stats.endTime.getTime() - this.stats.startTime.getTime()) /
            1000,
        )
      : 0;

    console.log('\n' + '='.repeat(50));
    console.log('         SYNC SUMMARY');
    console.log('='.repeat(50));
    console.log(`Duration:           ${duration}s`);
    console.log(`Brands checked:     ${this.stats.brandsChecked}`);
    console.log(`Brands created:     ${this.stats.brandsCreated}`);
    console.log(`Brands updated:     ${this.stats.brandsUpdated}`);
    console.log(`Users checked:      ${this.stats.usersChecked}`);
    console.log(`Users updated:      ${this.stats.usersUpdated}`);
    console.log(`Votes checked:      ${this.stats.votesChecked}`);
    console.log(`Votes inserted:     ${this.stats.votesInserted}`);
    console.log(`Fees processed:     ${this.stats.feesProcessed}`);
    console.log(`Fees succeeded:     ${this.stats.feesSucceeded}`);
    console.log(`Fees failed:        ${this.stats.feesFailed}`);
    console.log(`Errors:             ${this.stats.errors.length}`);
    console.log('='.repeat(50));

    if (this.stats.errors.length > 0) {
      console.log('\n⚠️ Errors:');
      this.stats.errors.slice(0, 10).forEach((err, i) => {
        console.log(`   ${i + 1}. ${err}`);
      });
      if (this.stats.errors.length > 10) {
        console.log(`   ... and ${this.stats.errors.length - 10} more`);
      }
    } else {
      console.log('\n✅ No errors!');
    }
  }
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  const syncer = new IndexerSyncer();

  try {
    // Check for CLI args (for non-interactive mode)
    const args = process.argv.slice(2);

    let options: SyncOptions;

    const dryRun = args.includes('--dry-run');

    if (args.includes('--48h')) {
      options = { windowHours: 48, syncBrands: true, syncPowerLevels: true, syncVotes: true, dryRun };
      console.log(`\n🔄 Running 48-hour sync (non-interactive mode)${dryRun ? ' [DRY RUN]' : ''}`);
    } else if (args.includes('--full')) {
      options = { windowHours: 0, syncBrands: true, syncPowerLevels: true, syncVotes: true, dryRun };
      console.log(`\n🔄 Running FULL sync (non-interactive mode)${dryRun ? ' [DRY RUN]' : ''}`);
    } else if (args.includes('--7d')) {
      options = { windowHours: 168, syncBrands: true, syncPowerLevels: true, syncVotes: true, dryRun };
      console.log(`\n🔄 Running 7-day sync (non-interactive mode)${dryRun ? ' [DRY RUN]' : ''}`);
    } else {
      // Interactive mode
      options = await promptForOptions();
    }

    await syncer.connect();
    await syncer.sync(options);
    syncer.printSummary();
  } catch (error) {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await syncer.disconnect();
    rl.close();
  }
}

main();
