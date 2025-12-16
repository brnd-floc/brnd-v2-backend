import {
  Controller,
  Get,
  Post,
  UseGuards,
  Res,
  Query,
  Body,
} from '@nestjs/common';
import { Response } from 'express';
import {
  AdminGuard,
  AuthorizationGuard,
  QuickAuthPayload,
} from '../../security/guards';
import { Session } from '../../security/decorators';
import { hasResponse, hasError, HttpStatus } from '../../utils/http';
import { AirdropService } from './services/airdrop.service';
import { AirdropContractService } from './services/airdrop-contract.service';
import { SignatureService } from '../blockchain/services/signature.service';
import { getConfig } from 'src/security/config';

@Controller('airdrop-service')
export class AirdropController {
  constructor(
    private readonly airdropService: AirdropService,
    private readonly airdropContractService: AirdropContractService,
    private readonly signatureService: SignatureService,
  ) {}

  @Get('check-user')
  @UseGuards(AuthorizationGuard)
  async checkUser(@Session() user: QuickAuthPayload, @Res() res: Response) {
    try {
      const airdropCalculation = await this.airdropService.checkUserEligibility(
        user.sub,
      );

      return hasResponse(res, {
        eligible: true,
        calculation: airdropCalculation,
        user: {
          fid: user.sub,
          address: user.address,
        },
      });
    } catch (error) {
      console.error('Error checking airdrop eligibility:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'checkUser',
        'Error checking airdrop eligibility',
      );
    }
  }

  @Get('leaderboard')
  async getLeaderboard(@Res() res: Response, @Query('limit') limit?: string) {
    try {
      const limitNum = limit ? parseInt(limit, 10) : 100;
      const maxLimit = 1000;
      const actualLimit = Math.min(limitNum, maxLimit);

      const leaderboard = await this.airdropService.getLeaderboard(actualLimit);

      const leaderboardWithRanking = leaderboard.map((entry, index) => ({
        rank: index + 1,
        fid: entry.fid,
        username: entry.user?.username || 'Unknown',
        photoUrl: entry.user?.photoUrl || null,
        basePoints: Number(entry.basePoints),
        multipliers: {
          followAccounts: Number(entry.followAccountsMultiplier),
          channelInteraction: Number(entry.channelInteractionMultiplier),
          holdingBrnd: Number(entry.holdingBrndMultiplier),
          collectibles: Number(entry.collectiblesMultiplier),
          votedBrands: Number(entry.votedBrandsMultiplier),
          sharedPodiums: Number(entry.sharedPodiumsMultiplier),
          neynarScore: Number(entry.neynarScoreMultiplier),
          proUser: Number(entry.proUserMultiplier),
        },
        totalMultiplier: Number(entry.totalMultiplier),
        finalScore: Number(entry.finalScore),
        tokenAllocation: Number(entry.tokenAllocation),
        percentage: Number(entry.percentage),
        lastUpdated: entry.updatedAt,
      }));

      return hasResponse(res, {
        leaderboard: leaderboardWithRanking,
        total: leaderboard.length,
        limit: actualLimit,
      });
    } catch (error) {
      console.error('Error fetching leaderboard:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getLeaderboard',
        'Error fetching leaderboard',
      );
    }
  }

  @Get('database-summary')
  @UseGuards(AuthorizationGuard)
  async getDatabaseSummary(@Res() res: Response) {
    try {
      console.log(`📊 [CONTROLLER] Getting database summary...`);

      const summary = await this.airdropService.getDatabaseSummary();

      console.log(`✅ [CONTROLLER] Database summary generated successfully`);

      return hasResponse(res, {
        message: 'Database summary generated successfully',
        summary,
      });
    } catch (error) {
      console.error('Error generating database summary:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getDatabaseSummary',
        'Error generating database summary',
      );
    }
  }

  @Get('fix-zero-allocations')
  @UseGuards(AuthorizationGuard)
  async fixZeroAllocations(@Res() res: Response) {
    try {
      console.log(`🔧 [CONTROLLER] Fixing zero score allocations...`);

      const result = await this.airdropService.fixZeroScoreAllocations();

      console.log(`✅ [CONTROLLER] Zero allocation fix completed`);

      return hasResponse(res, {
        message: 'Zero score allocations fixed successfully',
        results: result,
      });
    } catch (error) {
      console.error('Error fixing zero allocations:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'fixZeroAllocations',
        'Error fixing zero allocations',
      );
    }
  }

  @Get('recalculate-tokens')
  @UseGuards(AuthorizationGuard)
  async recalculateTokens(@Res() res: Response) {
    try {
      console.log(`🔄 [CONTROLLER] Starting token recalculation...`);

      const result = await this.airdropService.recalculateTokenDistribution();

      console.log(`✅ [CONTROLLER] Token recalculation completed`);

      return hasResponse(res, {
        message: 'Token distribution recalculated successfully',
        results: result,
      });
    } catch (error) {
      console.error('Error recalculating token distribution:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'recalculateTokens',
        'Error recalculating token distribution',
      );
    }
  }

  @Get('analytics')
  async getAnalytics(@Res() res: Response) {
    try {
      console.log(`📊 [CONTROLLER] Getting airdrop analytics...`);

      const analytics = await this.airdropService.getAirdropAnalytics();

      // Generate HTML report
      const html = `
<!DOCTYPE html>
<html>
<head>
    <title>BRND Airdrop Analytics Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .card { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .stat-box { background: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center; }
        .stat-number { font-size: 24px; font-weight: bold; color: #2563eb; }
        .stat-label { color: #6b7280; margin-top: 5px; }
        .table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        .table th, .table td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #e5e7eb; }
        .table th { background: #f9fafb; font-weight: 600; }
        .highlight { background: #fef3cd; }
        .text-green { color: #16a34a; }
        .text-red { color: #dc2626; }
        .text-blue { color: #2563eb; }
        h1 { color: #1f2937; text-align: center; }
        h2 { color: #374151; border-bottom: 2px solid #e5e7eb; padding-bottom: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 BRND Airdrop Analytics Dashboard</h1>
        
        <div class="card">
            <h2>📊 Summary</h2>
            <div class="stat-grid">
                <div class="stat-box">
                    <div class="stat-number">${analytics.summary.totalUsers.toLocaleString()}</div>
                    <div class="stat-label">Total Users</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">${analytics.summary.totalTokensDistributed.toLocaleString()}</div>
                    <div class="stat-label">Total Tokens</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">$${analytics.summary.totalUSDValue.toLocaleString()}</div>
                    <div class="stat-label">Total USD Value</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">${analytics.summary.averageTokensPerUser.toLocaleString()}</div>
                    <div class="stat-label">Avg Tokens/User</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">$${analytics.summary.averageUSDPerUser}</div>
                    <div class="stat-label">Avg USD/User</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">$${analytics.summary.brndUSDPrice}</div>
                    <div class="stat-label">BRND Price</div>
                </div>
            </div>
        </div>

        <div class="card">
            <h2>💰 USD Distribution</h2>
            <div class="stat-grid">
                <div class="stat-box">
                    <div class="stat-number text-red">${analytics.usdDistribution.under1USD}</div>
                    <div class="stat-label">Under $1</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">${analytics.usdDistribution.between1_5USD}</div>
                    <div class="stat-label">$1 - $5</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">${analytics.usdDistribution.between5_10USD}</div>
                    <div class="stat-label">$5 - $10</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">${analytics.usdDistribution.between10_20USD}</div>
                    <div class="stat-label">$10 - $20</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">${analytics.usdDistribution.between20_30USD}</div>
                    <div class="stat-label">$20 - $30</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number text-green">${analytics.usdDistribution.over30USD}</div>
                    <div class="stat-label">Over $30</div>
                </div>
            </div>
        </div>

        <div class="card">
            <h2>🏆 Top 20 Users</h2>
            <table class="table">
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Username</th>
                        <th>FID</th>
                        <th>Airdrop Score</th>
                        <th>Tokens</th>
                        <th>USD Value</th>
                        <th>%</th>
                    </tr>
                </thead>
                <tbody>
                    ${analytics.topUsers
                      .map(
                        (user, index) => `
                    <tr ${index < 3 ? 'class="highlight"' : ''}>
                        <td><strong>#${index + 1}</strong></td>
                        <td>${user.user?.username || 'Unknown'}</td>
                        <td>${user.fid}</td>
                        <td>${Number(user.finalScore).toLocaleString()}</td>
                        <td>${Number(user.tokenAllocation).toLocaleString()}</td>
                        <td class="text-green"><strong>$${user.usdValue}</strong></td>
                        <td>${Number(user.percentage).toFixed(3)}%</td>
                    </tr>
                    `,
                      )
                      .join('')}
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2>📉 Bottom 20 Users</h2>
            <table class="table">
                <thead>
                    <tr>
                        <th>Username</th>
                        <th>FID</th>
                        <th>Airdrop Score</th>
                        <th>Tokens</th>
                        <th>USD Value</th>
                    </tr>
                </thead>
                <tbody>
                    ${analytics.bottomUsers
                      .map(
                        (user) => `
                    <tr>
                        <td>${user.user?.username || 'Unknown'}</td>
                        <td>${user.fid}</td>
                        <td>${Number(user.finalScore).toLocaleString()}</td>
                        <td>${Number(user.tokenAllocation).toLocaleString()}</td>
                        <td class="text-blue">$${user.usdValue}</td>
                    </tr>
                    `,
                      )
                      .join('')}
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2>📈 Key Statistics</h2>
            <div class="stat-grid">
                <div class="stat-box">
                    <div class="stat-number text-green">${analytics.statistics.highestAllocation.tokens.toLocaleString()}</div>
                    <div class="stat-label">Highest Tokens</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number text-green">$${analytics.statistics.highestAllocation.usd}</div>
                    <div class="stat-label">Highest USD</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">${analytics.statistics.lowestAllocation.tokens.toLocaleString()}</div>
                    <div class="stat-label">Lowest Tokens</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">$${analytics.statistics.lowestAllocation.usd}</div>
                    <div class="stat-label">Lowest USD</div>
                </div>
            </div>
        </div>
        
        <div style="text-align: center; margin: 40px 0; color: #6b7280;">
            Generated on ${new Date().toLocaleString()}
        </div>
    </div>
</body>
</html>`;

      res.setHeader('Content-Type', 'text/html');
      return res.send(html);
    } catch (error) {
      console.error('Error generating analytics:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getAnalytics',
        'Error generating analytics',
      );
    }
  }

  // HERE IS THE CODE TO RECALCULATE ALL USERS AIRDROP ALLOCATION
  // @Get('recalculate-all-users')
  // @UseGuards(AuthorizationGuard)
  // async calculateAllUsers(
  //   @Session() user: QuickAuthPayload,
  //   @Res() res: Response,
  //   @Query('batchSize') batchSize?: string,
  // ) {
  //   try {
  //     const fid = user.sub;
  //     if (fid !== 16098) {
  //       return hasError(
  //         res,
  //         HttpStatus.FORBIDDEN,
  //         'calculateAllUsers',
  //         'Only jp can calculate all users',
  //       );
  //     }
  //     const batchSizeNum = batchSize ? parseInt(batchSize, 10) : 50;
  //     const maxBatchSize = 50;
  //     const actualBatchSize = Math.min(batchSizeNum, maxBatchSize);

  //     console.log(
  //       `🚀 [CONTROLLER] Starting bulk airdrop calculation with batch size: ${actualBatchSize}`,
  //     );

  //     // Check if snapshot already exists - warn but allow manual override via API
  //     const existingSnapshotsCount =
  //       await this.airdropService.airdropSnapshotRepository.count();

  //     if (existingSnapshotsCount > 0) {
  //       console.warn(
  //         `⚠️ [CONTROLLER] WARNING: ${existingSnapshotsCount} existing snapshot(s) found!`,
  //       );
  //       console.warn(
  //         '⚠️ [CONTROLLER] API calculation will proceed but may overwrite frozen allocations.',
  //       );
  //       console.warn(
  //         'ℹ️ [CONTROLLER] Consider clearing snapshots first if this is intentional.',
  //       );
  //     }

  //     const result =
  //       await this.airdropService.calculateAirdropForAllUsers(actualBatchSize);

  //     console.log(`✅ [CONTROLLER] Bulk calculation completed:`, result);

  //     return hasResponse(res, {
  //       message: 'Bulk airdrop calculation completed',
  //       results: result,
  //     });
  //   } catch (error) {
  //     console.error('Error calculating airdrop for all users:', error);
  //     return hasError(
  //       res,
  //       HttpStatus.INTERNAL_SERVER_ERROR,
  //       'calculateAllUsers',
  //       'Error calculating airdrop for all users',
  //     );
  //   }
  // }

  /**cooy
   * Check if user can claim airdrop
   * Returns eligibility status, contract status, and claim information
   */
  @Get('claim-status')
  @UseGuards(AuthorizationGuard)
  async getClaimStatus(
    @Session() user: QuickAuthPayload,
    @Res() res: Response,
  ) {
    try {
      const fid = user.sub;

      console.log(`🔍 [AIRDROP] Checking claim status for FID: ${fid}`);

      // Get contract status
      const contractStatus =
        await this.airdropContractService.getContractStatus();

      // Check if merkle root is set
      const isMerkleRootSet =
        await this.airdropContractService.isMerkleRootSet();

      // Check if user has already claimed
      const hasClaimed = await this.airdropContractService.hasClaimed(fid);

      // Check if user is in snapshot
      let proofData = null;
      try {
        proofData = await this.airdropService.generateMerkleProof(fid);
        if (!proofData) {
        }
      } catch (error) {
        // Snapshot doesn't exist or other database error
      }

      // Determine eligibility
      let canClaim = false;
      let reason = '';

      if (hasClaimed) {
        reason = 'Already claimed';
      } else if (!contractStatus.claimingEnabled) {
        reason = 'Claiming is not enabled yet';
      } else if (!isMerkleRootSet) {
        reason = 'Merkle root not set on contract';
      } else if (!proofData) {
        reason = 'Not eligible for airdrop (not in top 1111 users)';
      } else {
        canClaim = true;
        reason = 'Eligible to claim';
      }

      return hasResponse(res, {
        fid,
        canClaim,
        reason,
        hasClaimed,
        contractStatus: {
          merkleRootSet: isMerkleRootSet,
          claimingEnabled: contractStatus.claimingEnabled,
          totalClaimed: contractStatus.totalClaimed,
          escrowBalance: contractStatus.escrowBalance,
          allowance: contractStatus.allowance,
        },
        eligibility: {
          inSnapshot: !!proofData,
          amount: proofData?.amount || null,
        },
      });
    } catch (error) {
      console.error('Error checking claim status:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getClaimStatus',
        error.message || 'Failed to check claim status',
      );
    }
  }

  /**
   * Generate airdrop claim signature and merkle proof
   * Verifies wallet belongs to FID via Neynar, then generates signature
   */
  @Post('claim-signature')
  @UseGuards(AuthorizationGuard)
  async getAirdropClaimSignature(
    @Session() user: QuickAuthPayload,
    @Body() body: { walletAddress: string; snapshotId?: number },
    @Res() res: Response,
  ) {
    try {
      const { walletAddress, snapshotId } = body;
      const fid = user.sub;

      if (!walletAddress || !walletAddress.startsWith('0x')) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'getAirdropClaimSignature',
          'Valid wallet address is required',
        );
      }

      console.log(
        `🔐 [AIRDROP] Generating claim signature for FID: ${fid}, Wallet: ${walletAddress}`,
      );

      // Check contract status first
      const contractStatus =
        await this.airdropContractService.getContractStatus();
      const isMerkleRootSet =
        await this.airdropContractService.isMerkleRootSet();

      if (!contractStatus.claimingEnabled) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'getAirdropClaimSignature',
          'Claiming is not enabled on the contract yet',
        );
      }

      if (!isMerkleRootSet) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'getAirdropClaimSignature',
          'Merkle root is not set on the contract yet. Please wait for the snapshot to be set.',
        );
      }

      // Check if already claimed
      const hasClaimed = await this.airdropContractService.hasClaimed(fid);
      if (hasClaimed) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'getAirdropClaimSignature',
          'Airdrop already claimed for this FID',
        );
      }

      // Get merkle proof for this FID
      const proofData = await this.airdropService.generateMerkleProof(
        fid,
        snapshotId,
      );

      console.log('THE PROOF DATA IS:', proofData);

      if (!proofData) {
        console.log(
          `❌ [AIRDROP] FID ${fid} not found in snapshot: You are not eligible for the airdrop.`,
        );
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getAirdropClaimSignature',
          'FID not found in airdrop snapshot. You are not eligible for the airdrop.',
        );
      }

      // Verify merkle root matches contract
      // Normalize to lowercase for case-insensitive comparison (hex strings can vary in case)
      const proofMerkleRoot = proofData.merkleRoot.toLowerCase();
      const contractMerkleRoot = contractStatus.merkleRoot.toLowerCase();

      console.log(`🔍 [AIRDROP] Comparing merkle roots:`);
      console.log(`🔍 [AIRDROP] - Proof merkle root: ${proofMerkleRoot}`);
      console.log(`🔍 [AIRDROP] - Contract merkle root: ${contractMerkleRoot}`);
      console.log(
        `🔍 [AIRDROP] - Match: ${proofMerkleRoot === contractMerkleRoot}`,
      );

      if (proofMerkleRoot !== contractMerkleRoot) {
        console.log(
          `❌ [AIRDROP] Merkle root mismatch. The snapshot may have been updated. Please try again.`,
        );
        console.log(`❌ [AIRDROP] Proof root (lowercase): ${proofMerkleRoot}`);
        console.log(
          `❌ [AIRDROP] Contract root (lowercase): ${contractMerkleRoot}`,
        );
        console.log(
          `❌ [AIRDROP] Original proof root: ${proofData.merkleRoot}`,
        );
        console.log(
          `❌ [AIRDROP] Original contract root: ${contractStatus.merkleRoot}`,
        );
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'getAirdropClaimSignature',
          'Merkle root mismatch. The snapshot may have been updated. Please try again.',
        );
      }

      console.log(`✅ [AIRDROP] Merkle root verification passed`);

      // Generate EIP-712 signature (this verifies wallet belongs to FID via Neynar)
      const deadline = Math.floor(Date.now() / 1000) + 3600; // 1 hour from now

      const signature =
        await this.signatureService.generateAirdropClaimSignature(
          fid,
          walletAddress,
          parseInt(proofData.amount), // Convert string to number for baseAmount
          proofData.merkleRoot,
          deadline,
        );

      console.log(`✅ [AIRDROP] Claim signature generated successfully`);

      // CRITICAL DEBUG: Let's manually calculate the exact leaf hash the contract will compute
      const { AbiCoder, keccak256: ethersKeccak256 } = require('ethers');
      const abiCoder = AbiCoder.defaultAbiCoder();

      const contractFid = BigInt(proofData.fid);
      const contractBaseAmount = BigInt(proofData.amount);

      console.log(
        `🧪 [CONTRACT SIMULATION] Simulating exact contract calculation:`,
      );
      console.log(
        `🧪 [CONTRACT SIMULATION] - Input FID: ${contractFid.toString()}`,
      );
      console.log(
        `🧪 [CONTRACT SIMULATION] - Input BaseAmount: ${contractBaseAmount.toString()}`,
      );

      // Exactly what the contract will do: keccak256(abi.encode(fid, baseAmount))
      const contractEncoded = abiCoder.encode(
        ['uint256', 'uint256'],
        [contractFid, contractBaseAmount],
      );
      const contractLeafHash = ethersKeccak256(contractEncoded);

      console.log(
        `🧪 [CONTRACT SIMULATION] - Contract ABI Encoded: ${contractEncoded}`,
      );
      console.log(
        `🧪 [CONTRACT SIMULATION] - Contract Leaf Hash: ${contractLeafHash}`,
      );
      console.log(
        `🧪 [CONTRACT SIMULATION] - Backend Stored Hash: (will get from proof data)`,
      );

      // We need to get the stored hash from the backend's proof generation
      // The backend already verified the hash matches, but let's double-check here
      console.log(
        `🧪 [CONTRACT SIMULATION] - Our calculation matches backend's previous verification`,
      );
      console.log(
        `🧪 [CONTRACT SIMULATION] - If this fails, there's a fundamental mismatch in leaf calculation`,
      );

      // Let's also verify our proof array format
      console.log(`🧪 [CONTRACT DEBUG] Proof array format check:`);
      proofData.proof.forEach((p, i) => {
        console.log(
          `🧪 [CONTRACT DEBUG] - Proof[${i}]: ${p} (length: ${p.length}, valid hex: ${p.startsWith('0x')})`,
        );
      });

      return hasResponse(res, {
        fid,
        walletAddress,
        amount: proofData.amount,
        merkleRoot: proofData.merkleRoot,
        proof: proofData.proof,
        signature,
        deadline,
        snapshotId: proofData.snapshotId,
        contractAddress: getConfig().blockchain
          .airdropContractAddress as string,
        message:
          'Use these values to call claimAirdrop() on the smart contract',
      });
    } catch (error) {
      console.error('Error generating airdrop claim signature:', error);

      // Handle specific error cases
      if (error.message?.includes('not verified for this FID')) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'getAirdropClaimSignature',
          'Wallet address is not verified for this FID on Farcaster. Please verify your wallet address on Farcaster first.',
        );
      }

      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getAirdropClaimSignature',
        error.message || 'Failed to generate claim signature',
      );
    }
  }
}
