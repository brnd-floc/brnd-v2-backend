// Dependencies
import {
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  Res,
  UseGuards,
} from '@nestjs/common';

import { logger } from '../../main';
import { ApiTags } from '@nestjs/swagger';
import { Response } from 'express';

// Services
import { BrandOrderType, BrandResponse, BrandService } from './services';
import { BrandSeederService } from './services/brand-seeding.service';
import { UserService } from '../user/services/user.service';
import { RewardService } from '../blockchain/services/reward.service';

// Models
import { Brand, CurrentUser } from '../../models';

// Utils
import { HttpStatus, hasError, hasResponse } from '../../utils';

// Security
import { AuthorizationGuard, QuickAuthPayload } from '../../security/guards';
import { Session } from '../../security/decorators';
import { getConfig } from '../../security/config';
import NeynarService from 'src/utils/neynar';
import { BrandSchedulerService } from './services/brand-scheduler.service';

export type BrandTimePeriod = 'day' | 'week' | 'month' | 'all';

@ApiTags('brand-service')
@Controller('brand-service')
export class BrandController {
  constructor(
    private readonly brandService: BrandService,
    private readonly brandSeederService: BrandSeederService,
    private readonly userService: UserService,
    private readonly rewardService: RewardService,
    private readonly brandSchedulerService: BrandSchedulerService,
  ) {}

  @Get('/brand/:id')
  async getBrandById(
    @Param('id') id: Brand['id'],
  ): Promise<BrandResponse | undefined> {
    return this.brandService.getById(id, [], ['category']);
  }

  @Get('/brand/:id/enhanced')
  async getEnhancedBrandInfo(
    @Param('id') id: Brand['id'],
    @Res() res: Response,
  ): Promise<Response> {
    try {
      const brandResponse = await this.brandService.getById(
        id,
        [],
        ['category'],
      );
      if (!brandResponse) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getEnhancedBrandInfo',
          'Brand not found',
        );
      }

      const enhancedBrand = {
        ...brandResponse.brand,
        onChain: {
          fid: brandResponse.brand.onChainFid,
          walletAddress: brandResponse.brand.walletAddress,
          totalBrndAwarded: brandResponse.brand.totalBrndAwarded,
          availableBrnd: brandResponse.brand.availableBrnd,
          handle: brandResponse.brand.onChainHandle,
          metadataHash: brandResponse.brand.metadataHash,
          createdAt: brandResponse.brand.onChainCreatedAt?.getTime() || null,
        },
        casts: brandResponse.casts,
        fanCount: brandResponse.fanCount,
      };

      return hasResponse(res, enhancedBrand);
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getEnhancedBrandInfo',
        'Failed to retrieve enhanced brand information',
      );
    }
  }

  @Post('/brand/:brandId/withdraw')
  @UseGuards(AuthorizationGuard)
  async initiateBrandWithdrawal(
    @Session() session: QuickAuthPayload,
    @Param('brandId') brandId: Brand['id'],
    @Body() { requesterAddress }: { requesterAddress: string },
    @Res() res: Response,
  ): Promise<Response> {
    try {
      logger.log(
        `💰 [BRAND] Withdrawal request for brand ${brandId} by FID: ${session.sub}`,
      );

      if (!requesterAddress) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'initiateBrandWithdrawal',
          'Requester address is required',
        );
      }

      // Get brand information
      const brandResponse = await this.brandService.getById(brandId);
      if (!brandResponse) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'initiateBrandWithdrawal',
          'Brand not found',
        );
      }

      const brand = brandResponse.brand;

      // Check if requester has permission (brand owner FID or wallet address)
      const hasPermission =
        brand.onChainFid === session.sub ||
        brand.walletAddress?.toLowerCase() === requesterAddress.toLowerCase();

      if (!hasPermission) {
        return hasError(
          res,
          HttpStatus.FORBIDDEN,
          'initiateBrandWithdrawal',
          'You do not have permission to withdraw rewards for this brand',
        );
      }

      // Return withdrawal information (in a real implementation, this would trigger the smart contract withdrawal)
      return hasResponse(res, {
        brandId: brand.id,
        brandName: brand.name,
        availableBrnd: brand.availableBrnd,
        totalBrndAwarded: brand.totalBrndAwarded,
        walletAddress: brand.walletAddress,
        requesterAddress,
        canWithdraw: parseFloat(brand.availableBrnd) > 0,
        message:
          parseFloat(brand.availableBrnd) > 0
            ? 'Withdrawal can be initiated on-chain'
            : 'No rewards available for withdrawal',
      });
    } catch (error) {
      logger.error('Failed to initiate brand withdrawal:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'initiateBrandWithdrawal',
        'Failed to process withdrawal request',
      );
    }
  }

  @Get('/list')
  @UseGuards(AuthorizationGuard)
  async getAllBrands(
    @Query('order') order: BrandOrderType,
    @Query('period') period: BrandTimePeriod = 'all',
    @Query('search') search: string,
    @Query('pageId') pageId: number,
    @Query('limit') limit: number,
    @Res() res: Response,
  ) {
    const [brands, count] = await this.brandService.getAll(
      [
        'id',
        'name',
        'url',
        'imageUrl',
        'profile',
        'channel',
        'stateScore',
        'score',
        'ranking',
        'scoreWeek',
        'stateScoreWeek',
        'rankingWeek',
        'scoreMonth',
        'stateScoreMonth',
        'rankingMonth',
        'banned',
        'scoreDay',
      ],
      [],
      order,
      period,
      search,
      pageId,
      limit,
    );

    return hasResponse(res, {
      pageId,
      count,
      brands,
    });
  }

  @Post('/verify-share')
  @UseGuards(AuthorizationGuard)
  async verifyShare(
    @Session() user: QuickAuthPayload,
    @Body()
    {
      castHash,
      voteId,
      recipientAddress,
      transactionHash,
    }: {
      castHash: string;
      voteId: string;
      recipientAddress?: string;
      transactionHash?: string;
    },
    @Res() res: Response,
  ): Promise<Response> {
    try {
      if (!voteId) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Vote ID is required',
        );
      }

      const isClaimRetrieval = !castHash || castHash.trim() === '';

      if (isClaimRetrieval) {
        return await this.handleClaimRetrieval(
          user,
          voteId,
          recipientAddress,
          res,
        );
      }

      if (recipientAddress && !/^0x[a-fA-F0-9]{40}$/.test(recipientAddress)) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Invalid recipient address format',
        );
      }

      if (!/^0x[a-fA-F0-9]{40}$/.test(castHash)) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Invalid cast hash format',
        );
      }

      const dbUser = await this.userService.getByFid(user.sub);
      if (!dbUser) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'User not found',
        );
      }

      const vote = await this.brandService.getVoteByTransactionHash(
        transactionHash as string,
      );

      if (!vote) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'Vote not found',
        );
      }

      if (vote.user.fid !== dbUser.fid) {
        return hasError(
          res,
          HttpStatus.FORBIDDEN,
          'verifyShare',
          'Vote does not belong to user',
        );
      }

      if (vote.shared) {
        return hasError(
          res,
          HttpStatus.CONFLICT,
          'verifyShare',
          'Vote has already been shared',
        );
      }

      try {
        const neynar = new NeynarService();
        const castData = await neynar.getCastByHash(castHash);

        if (castData.author.fid !== user.sub) {
          return hasError(
            res,
            HttpStatus.FORBIDDEN,
            'verifyShare',
            'Cast was not posted by the authenticated user',
          );
        }

        const validEmbedUrls = [
          'https://brnd.land',
          'https://rebrnd.lat',
          'https://www.brnd.land',
          'https://poiesis.anky.app',
        ];

        const correctEmbedIndex = castData.embeds.findIndex((embed) => {
          if ('url' in embed) {
            return validEmbedUrls.some((baseUrl) =>
              embed.url.includes(baseUrl),
            );
          }
          return false;
        });

        if (correctEmbedIndex === -1) {
          return hasError(
            res,
            HttpStatus.BAD_REQUEST,
            'verifyShare',
            'Cast does not contain the correct embed URL',
          );
        }

        const correctEmbed = castData.embeds[correctEmbedIndex] as any;
        const correctEmbedUrl = correctEmbed.url;
        const transactionHashFromQueryParam =
          correctEmbedUrl.split('/podium/')[1];

        if (vote.transactionHash !== transactionHashFromQueryParam) {
          return hasError(
            res,
            HttpStatus.BAD_REQUEST,
            'verifyShare',
            'Cast does not contain the correct tx hash',
          );
        }

        await this.brandService.markVoteAsShared(
          vote.transactionHash,
          castHash,
        );
        const updatedUser = await this.userService.addPoints(dbUser.id, 3);

        const voteTimestamp = Math.floor(new Date(vote.date).getTime() / 1000);
        const day = Math.floor(voteTimestamp / 86400);

        await this.rewardService.verifyShareForReward(
          dbUser.fid,
          day,
          castHash,
        );

        let claimSignature = null;
        if (recipientAddress) {
          try {
            claimSignature = await this.rewardService.generateClaimSignature(
              dbUser.fid,
              day,
              recipientAddress,
              castHash,
            );
          } catch (claimError) {
            // Do nothing, skip claim sig on error
          }
        }

        const responsePayload = {
          verified: true,
          pointsAwarded: 3,
          newTotalPoints: updatedUser.points,
          message: 'Share verified successfully! 3 points awarded.',
          day,
          claimSignature: claimSignature
            ? {
                signature: claimSignature.signature,
                amount: claimSignature.amount,
                deadline: claimSignature.deadline,
                nonce: claimSignature.nonce,
                canClaim: claimSignature.canClaim,
              }
            : null,
          note: recipientAddress
            ? 'Claim signature generated. You can now claim your reward on-chain.'
            : 'Provide recipientAddress to generate claim signature.',
        };

        try {
          const pointsForVote = 6 + updatedUser.brndPowerLevel * 3;
          const config = getConfig();
          if (config.neynar.apiKey && config.neynar.signerUuid) {
            await fetch('https://api.neynar.com/v2/farcaster/cast', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'x-api-key': config.neynar.apiKey,
              },
              body: JSON.stringify({
                signer_uuid: config.neynar.signerUuid,
                embeds: [
                  { cast_id: { hash: castHash, fid: castData.author.fid } },
                ],

                text: `Thank you for voting @${castData.author.username}. Your vote has been verified. You earned ${pointsForVote} points and now have a total of ${updatedUser.points} points.\n\nYou can now claim ${vote.brndPaidWhenCreatingPodium * 10} $BRND on the miniapp.`,
              }),
            });
          }
        } catch (replyError) {
          // Do nothing if reply failed
        }

        return hasResponse(res, {
          ...responsePayload,
          castHash,
        });
      } catch (neynarError) {
        if (neynarError.message?.includes('Cast not found')) {
          return hasError(
            res,
            HttpStatus.NOT_FOUND,
            'verifyShare',
            'Cast not found on Farcaster',
          );
        }

        return hasError(
          res,
          HttpStatus.INTERNAL_SERVER_ERROR,
          'verifyShare',
          'Failed to verify cast with Farcaster',
        );
      }
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'verifyShare',
        'An unexpected error occurred during verification',
      );
    }
  }

  private async handleClaimRetrieval(
    user: QuickAuthPayload,
    voteId: string,
    recipientAddress: string,
    res: Response,
  ): Promise<Response> {
    try {
      if (recipientAddress && !/^0x[a-fA-F0-9]{40}$/.test(recipientAddress)) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Invalid recipient address format',
        );
      }

      const dbUser = await this.userService.getByFid(user.sub);
      if (!dbUser) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'User not found',
        );
      }

      const vote = await this.brandService.getVoteByTransactionHash(voteId);

      if (!vote) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'Vote not found',
        );
      }

      if (vote.user.fid !== dbUser.fid) {
        return hasError(
          res,
          HttpStatus.FORBIDDEN,
          'verifyShare',
          'Vote does not belong to user',
        );
      }

      let day: number;
      if (vote.day != null) {
        day = vote.day;
      } else {
        const voteTimestamp = Math.floor(new Date(vote.date).getTime() / 1000);
        day = Math.floor(voteTimestamp / 86400);
      }

      const existingShare =
        await this.brandService.getVerifiedShareByUserAndDay(user.sub, day);

      if (!existingShare) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'No verified share found for this vote.',
        );
      }

      let claimSignature = null;
      if (recipientAddress) {
        try {
          claimSignature = await this.rewardService.generateClaimSignature(
            dbUser.fid,
            day,
            recipientAddress,
            existingShare.castHash,
          );
        } catch (claimError) {
          // Do nothing, still return the share info
        }
      }

      const responsePayload = {
        verified: true,
        pointsAwarded: 0,
        newTotalPoints: dbUser.points,
        message: existingShare.claimedAt
          ? 'Share verified. Rewards already claimed.'
          : 'Share verified. Rewards available.',
        day,
        claimSignature: claimSignature
          ? {
              signature: claimSignature.signature,
              amount: claimSignature.amount,
              deadline: claimSignature.deadline,
              nonce: claimSignature.nonce,
              canClaim: claimSignature.canClaim,
            }
          : null,
        castHash: existingShare.castHash,
        note: recipientAddress
          ? existingShare.claimedAt
            ? 'Rewards have already been claimed for this share.'
            : 'Claim signature generated. You can claim your reward on-chain.'
          : 'Provide recipientAddress to generate claim signature.',
      };

      return hasResponse(res, responsePayload);
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'verifyShare',
        'An unexpected error occurred during claim retrieval',
      );
    }
  }

  @Post('/request')
  @UseGuards(AuthorizationGuard)
  async requestBrand(
    @Session() user: CurrentUser,
    @Body() { name }: { name: string },
    @Res() res: Response,
  ): Promise<Response> {
    try {
      // (Intentionally left blank: no-op for now, removed logs)
      return hasResponse(res, {});
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'requestBrand',
        error.toString(),
      );
    }
  }

  @Post('/:id/follow')
  @UseGuards(AuthorizationGuard)
  async followBrand(@Session() user: CurrentUser, @Param('id') id: string) {
    // (Intentionally left blank: no-op, removed logs)
  }

  @Get('/debug/scoring')
  async debugScoring(@Res() res: Response) {
    try {
      const debugInfo = await this.brandService.getDebugScoringInfo();

      return hasResponse(res, debugInfo);
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'debugScoring',
        error.message,
      );
    }
  }

  @Get('/cycles/:period/rankings')
  async getCycleRankings(
    @Param('period') period: 'week' | 'month',
    @Query('limit') limit: number = 10,
    @Res() res: Response,
  ) {
    if (period !== 'week' && period !== 'month') {
      return hasError(
        res,
        HttpStatus.BAD_REQUEST,
        'getCycleRankings',
        'Period must be "week" or "month"',
      );
    }

    try {
      const result = await this.brandService.getCycleRankings(period, limit);

      return hasResponse(res, {
        period,
        rankings: result.rankings,
        cycleInfo: result.cycleInfo,
        metadata: {
          generatedAt: new Date().toISOString(),
          totalBrands: result.rankings.length,
          cycleNumber: result.cycleInfo.cycleNumber,
        },
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getCycleRankings',
        error.message,
      );
    }
  }

  @Get('/deployment-info')
  async getDeploymentInfo(@Res() res: Response) {
    try {
      const deploymentInfo = await this.brandService.getDeploymentInfo();
      return hasResponse(res, deploymentInfo);
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getDeploymentInfo',
        error.message,
      );
    }
  }

  @Get('/weekly-leaderboard')
  async getWeeklyLeaderboard(
    @Query('week') week: string,
    @Query('limit') limit: number = 10,
    @Res() res: Response,
  ) {
    try {
      const result = await this.brandService.getWeeklyLeaderboard(week, limit);

      return hasResponse(res, {
        selectedWeek: week,
        leaderboard: result.leaderboard,
        weekPicker: result.weekPicker,
        metadata: {
          generatedAt: new Date().toISOString(),
          totalBrands: result.leaderboard.length,
          weekNumber: result.weekNumber,
          isCurrentWeek: result.isCurrentWeek,
        },
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getWeeklyLeaderboard',
        error.message,
      );
    }
  }

  @Get('/dev/seed')
  @UseGuards(AuthorizationGuard)
  async seedBrands(
    @Session() user: QuickAuthPayload,
    @Query('overwrite') overwrite: string = 'false',
    @Res() res: Response,
  ): Promise<Response> {
    const adminFids = [16098, 5431];
    if (!adminFids.includes(user.sub)) {
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'seedBrands',
        'Admin access required',
      );
    }
    try {
      const shouldOverwrite = overwrite.toLowerCase() === 'true';
      const result = await this.brandSeederService.seedBrands(shouldOverwrite);

      return hasResponse(res, {
        message: 'Brand seeding completed successfully',
        ...result,
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'seedBrands',
        `Seeding failed: ${error.message}`,
      );
    }
  }

  @Get('/dev/stats')
  @UseGuards(AuthorizationGuard)
  async getDatabaseStats(
    @Session() user: QuickAuthPayload,
    @Res() res: Response,
  ): Promise<Response> {
    const adminFids = [16098, 5431];
    if (!adminFids.includes(user.sub)) {
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'getDatabaseStats',
        'Admin access required',
      );
    }
    try {
      const stats = await this.brandSeederService.getStats();

      return hasResponse(res, {
        message: 'Database statistics retrieved successfully',
        ...stats,
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getDatabaseStats',
        `Failed to get stats: ${error.message}`,
      );
    }
  }

  @Get('/dev/preview')
  @UseGuards(AuthorizationGuard)
  async previewSeeding(
    @Session() user: QuickAuthPayload,
    @Res() res: Response,
  ): Promise<Response> {
    const adminFids = [16098, 5431];
    if (!adminFids.includes(user.sub)) {
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'previewSeeding',
        'Admin access required',
      );
    }
    try {
      const preview = await this.brandSeederService.previewSeeding();

      return hasResponse(res, {
        message: 'Seeding preview completed',
        ...preview,
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'previewSeeding',
        `Preview failed: ${error.message}`,
      );
    }
  }

  @Get('/recent-podiums')
  @UseGuards(AuthorizationGuard)
  async getRecentPodiums(
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 20,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      const [podiums, count] = await this.brandService.getRecentPodiums(
        page,
        limit,
      );

      return hasResponse(res, {
        podiums: podiums,
        pagination: {
          page,
          limit,
          total: count,
          totalPages: Math.ceil(count / limit),
          hasNextPage: page * limit < count,
          hasPrevPage: page > 1,
        },
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getRecentPodiums',
        'Failed to fetch recent podiums',
      );
    }
  }

  @Post('/dev/clear')
  @UseGuards(AuthorizationGuard)
  async clearAllBrands(
    @Session() user: QuickAuthPayload,
    @Query('confirm') confirm: string,
    @Res() res: Response,
  ): Promise<Response> {
    const adminFids = [16098, 5431];
    if (!adminFids.includes(user.sub)) {
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'clearAllBrands',
        'Admin access required',
      );
    }
    try {
      if (confirm !== 'yes') {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'clearAllBrands',
          'Must provide ?confirm=yes to clear all brands',
        );
      }

      const deletedCount = await this.brandSeederService.clearAllBrands();

      return hasResponse(res, {
        message: `Successfully cleared ${deletedCount} brands from database`,
        deletedCount,
        warning: 'This action cannot be undone',
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'clearAllBrands',
        `Failed to clear brands: ${error.message}`,
      );
    }
  }

  @Post('/dev/test-cron')
  @UseGuards(AuthorizationGuard)
  async testCronJobs(
    @Session() user: QuickAuthPayload,
    @Query('type') type: 'daily' | 'reminder' | 'health' = 'daily',
    @Res() res: Response,
  ): Promise<Response> {
    const adminFids = [16098, 5431];
    if (!adminFids.includes(user.sub)) {
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'testCronJobs',
        'Admin access required',
      );
    }

    try {
      switch (type) {
        case 'daily':
          await this.brandSchedulerService.handlePeriodEnd();
          break;
        case 'reminder':
          await this.brandSchedulerService.sendDailyVoteReminder();
          break;
        case 'health':
          await this.brandSchedulerService.healthCheck();
          break;
        default:
          return hasError(
            res,
            HttpStatus.BAD_REQUEST,
            'testCronJobs',
            'Invalid type. Use: daily, reminder, or health',
          );
      }

      return hasResponse(res, {
        message: `${type} cron job executed successfully`,
        executedAt: new Date().toISOString(),
        type,
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'testCronJobs',
        `Failed to execute ${type} cron job: ${error.message}`,
      );
    }
  }
}
