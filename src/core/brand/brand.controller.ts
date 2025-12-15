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

  /**
   * Retrieves a brand by its ID.
   *
   * @param {Brand['id']} id - The ID of the brand to retrieve.
   * @returns {Promise<BrandResponse | undefined>} The brand response with brand, casts, and fanCount or undefined if not found.
   */
  @Get('/brand/:id')
  async getBrandById(
    @Param('id') id: Brand['id'],
  ): Promise<BrandResponse | undefined> {
    return this.brandService.getById(id, [], ['category']);
  }

  /**
   * Retrieves enhanced brand information including on-chain data for V3 contract.
   *
   * @param {Brand['id']} id - The ID of the brand to retrieve.
   * @returns {Promise<BrandResponse>} The brand entity with on-chain information.
   */
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

  /**
   * Initiates brand reward withdrawal for V3 contract.
   *
   * @param {Brand['id']} brandId - The ID of the brand.
   * @param {string} requesterAddress - The address requesting withdrawal.
   * @returns {Promise<Response>} Withdrawal initiation result.
   */
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

  /**
   * Retrieves all brands with pagination.
   *
   * @param {BrandOrderType} order - The order in which to sort the brands.
   * @param {string} search - The search query to filter brands.
   * @param {number} pageId - The ID of the page to retrieve.
   * @param {number} limit - The number of brands to retrieve per page.
   * @param {Response} res - The response object.
   * @returns {Promise<Response>} A response object containing the page ID, total count of brands, and an array of brand objects.
   * @param {BrandTimePeriod} period - The time period to filter brands.
   */
  @Get('/list')
  @UseGuards(AuthorizationGuard)
  async getAllBrands(
    @Query('order') order: BrandOrderType,
    @Query('period') period: BrandTimePeriod = 'all', // NEW: Add period parameter
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
        'scoreMonth', // NEW
        'stateScoreMonth', // NEW
        'rankingMonth', // NEW
        'banned',
        'scoreDay',
      ],
      [],
      order,
      period, // NEW: Pass period to service
      search,
      pageId,
      limit,
    );
    console.log('BRANDS', brands);

    return hasResponse(res, {
      pageId,
      count,
      brands,
    });
  }

  // /**
  //  * Records votes for multiple brands.
  //  *
  //  * @param {CurrentUser} user - The current user session.
  //  * @param {{ ids: string[] }} ids - An object containing an array of brand IDs to vote for.
  //  * @param {Response} res - The response object.
  //  * @returns {Promise<Response>} A response object indicating the result of the vote operation.
  //  */
  // @Post('/vote')
  // @UseGuards(AuthorizationGuard)
  // async voteBrands(
  //   @Session() user: QuickAuthPayload, // Get FID from QuickAuth token
  //   @Body() { ids }: { ids: number[] },
  //   @Res() res: Response,
  // ): Promise<Response> {
  //   console.log(
  //     '🗳️ [VoteBrands] User FID:',
  //     user.sub,
  //     'attempting to vote with IDs:',
  //     ids.join(', '),
  //   );

  //   try {
  //     // Validate request body
  //     if (!ids || !Array.isArray(ids)) {
  //       return hasError(
  //         res,
  //         HttpStatus.BAD_REQUEST,
  //         'voteBrands',
  //         'Invalid request: ids must be an array',
  //       );
  //     }

  //     if (ids.length !== 3) {
  //       return hasError(
  //         res,
  //         HttpStatus.BAD_REQUEST,
  //         'voteBrands',
  //         'Invalid request: must provide exactly 3 brand IDs',
  //       );
  //     }

  //     // Check for duplicate IDs
  //     const uniqueIds = new Set(ids);
  //     if (uniqueIds.size !== 3) {
  //       return hasError(
  //         res,
  //         HttpStatus.BAD_REQUEST,
  //         'voteBrands',
  //         'Invalid request: all brand IDs must be different',
  //       );
  //     }

  //     console.log(
  //       '✅ [VoteBrands] Vote validation passed, submitting votes...',
  //     );

  //     // Use user.sub (FID) - the brandService will find the user in the database
  //     const votes = await this.brandService.voteForBrands(user.sub, ids);

  //     return hasResponse(res, {
  //       transactionHash: votes.transactionHash,
  //       date: votes.date,
  //       brand1: {
  //         id: votes.brand1.id,
  //         name: votes.brand1.name,
  //         imageUrl: votes.brand1.imageUrl,
  //       },
  //       brand2: {
  //         id: votes.brand2.id,
  //         name: votes.brand2.name,
  //         imageUrl: votes.brand2.imageUrl,
  //       },
  //       brand3: {
  //         id: votes.brand3.id,
  //         name: votes.brand3.name,
  //         imageUrl: votes.brand3.imageUrl,
  //       },
  //       message: 'Vote submitted successfully',
  //       bot_cast_hash: votes.bot_cast_hash,
  //     });
  //   } catch (error) {
  //     console.error('❌ [VoteBrands] Voting error:', error);

  //     // Handle specific error types
  //     if (error.message.includes('already voted')) {
  //       return hasError(res, HttpStatus.CONFLICT, 'voteBrands', error.message);
  //     }

  //     if (error.message.includes('not found')) {
  //       return hasError(res, HttpStatus.NOT_FOUND, 'voteBrands', error.message);
  //     }

  //     if (error.message.includes('do not exist')) {
  //       return hasError(
  //         res,
  //         HttpStatus.BAD_REQUEST,
  //         'voteBrands',
  //         error.message,
  //       );
  //     }

  //     return hasError(
  //       res,
  //       HttpStatus.INTERNAL_SERVER_ERROR,
  //       'voteBrands',
  //       'An unexpected error occurred while processing your vote',
  //     );
  //   }
  // }

  /**
   * Verifies a shared cast and awards points for valid shares.
   *
   * This endpoint validates that:
   * 1. The cast hash exists on Farcaster
   * 2. The cast was posted by the authenticated user
   * 3. The cast contains the correct embed URL
   * 4. The vote hasn't been shared before
   *
   * @param user - The authenticated user from QuickAuth
   * @param castHash - The Farcaster cast hash to verify (0x format)
   * @param transactionHash - The transaction hash of the vote being shared
   * @param res - The response object
   * @returns Verification result and updated user points
   */
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
      console.log(
        `🔍 [VerifyShare] User FID: ${user.sub}, Cast: ${castHash}, Vote: ${voteId}`,
      );

      // Validate input
      if (!voteId) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Vote ID is required',
        );
      }

      // Check if this is a request to retrieve claim signature for already shared vote
      const isClaimRetrieval = !castHash || castHash.trim() === '';

      if (isClaimRetrieval) {
        console.log(
          '🔄 [VerifyShare] Handling claim retrieval for already shared vote',
        );
        return await this.handleClaimRetrieval(
          user,
          voteId,
          recipientAddress,
          res,
        );
      }

      // Validate recipient address if provided
      if (recipientAddress && !/^0x[a-fA-F0-9]{40}$/.test(recipientAddress)) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Invalid recipient address format',
        );
      }

      // Validate cast hash format (should start with 0x and be 40 characters)
      if (!/^0x[a-fA-F0-9]{40}$/.test(castHash)) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Invalid cast hash format',
        );
      }
      console.log('CAST HASH', castHash);

      // Get the user from database
      const dbUser = await this.userService.getByFid(user.sub);
      if (!dbUser) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'User not found',
        );
      }

      console.log('DB USER', dbUser);

      // Get the vote and check if it belongs to the user
      const vote = await this.brandService.getVoteByTransactionHash(
        transactionHash as string,
      );

      console.log('VOTE', vote);
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

      // Check if vote has already been shared
      if (vote.shared) {
        return hasError(
          res,
          HttpStatus.CONFLICT,
          'verifyShare',
          'Vote has already been shared',
        );
      }

      // Verify cast with Neynar
      try {
        const neynar = new NeynarService();
        const castData = await neynar.getCastByHash(castHash);
        console.log('CAST DATA', castData, user);
        // Verify the cast author FID matches the user
        if (castData.author.fid !== user.sub) {
          return hasError(
            res,
            HttpStatus.FORBIDDEN,
            'verifyShare',
            'Cast was not posted by the authenticated user',
          );
        }

        // Verify the cast contains the correct embed URL
        // Accept both brnd.land or rebrnd.lat as valid base URLs in the embed
        const validEmbedUrls = [
          'https://brnd.land',
          'https://rebrnd.lat',
          'https://www.brnd.land',
        ];

        // Find embed that matches any of our valid URLs and has the url property
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

        // We know this embed has the url property since we checked above
        const correctEmbed = castData.embeds[correctEmbedIndex] as any;
        const correctEmbedUrl = correctEmbed.url;
        const transactionHashFromQueryParam =
          correctEmbedUrl.split('?txHash=')[1];
        if (vote.transactionHash !== transactionHashFromQueryParam) {
          return hasError(
            res,
            HttpStatus.BAD_REQUEST,
            'verifyShare',
            'Cast does not contain the correct tx hash',
          );
        }

        // All verifications passed - update vote and award points
        await this.brandService.markVoteAsShared(
          vote.transactionHash,
          castHash,
        );
        const updatedUser = await this.userService.addPoints(dbUser.id, 3);

        // Calculate day from vote date (using same calculation as contract: block.timestamp / 86400)
        const voteTimestamp = Math.floor(new Date(vote.date).getTime() / 1000);
        const day = Math.floor(voteTimestamp / 86400);

        console.log('Before verifying share for reward');

        // Mark share as verified for reward claim
        await this.rewardService.verifyShareForReward(
          dbUser.fid,
          day,
          castHash,
        );
        console.log('After verifying share for reward');

        // If recipient address is provided, generate the claim signature
        let claimSignature = null;
        if (recipientAddress) {
          console.log('Generating claim signature');
          try {
            claimSignature = await this.rewardService.generateClaimSignature(
              dbUser.fid,
              day,
              recipientAddress,
              castHash,
            );
            console.log('✅ [VerifyShare] Claim signature generated:', {
              signature: claimSignature.signature?.substring(0, 20) + '...',
              amount: claimSignature.amount,
              amountType: typeof claimSignature.amount,
              amountLength: claimSignature.amount.length,
              deadline: claimSignature.deadline,
              nonce: claimSignature.nonce,
              canClaim: claimSignature.canClaim,
            });
          } catch (claimError) {
            console.error(
              '❌ [VerifyShare] Failed to generate claim signature:',
              claimError,
            );
          }
        }

        // RIGHT BEFORE THE RETURN STATEMENT, ADD THIS:
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

        console.log('📤 [VerifyShare] Response payload:', {
          ...responsePayload,
          claimSignature: responsePayload.claimSignature
            ? {
                ...responsePayload.claimSignature,
                signature:
                  responsePayload.claimSignature.signature.substring(0, 20) +
                  '...',
                amount: responsePayload.claimSignature.amount,
                amountType: typeof responsePayload.claimSignature.amount,
                amountLength: String(responsePayload.claimSignature.amount)
                  .length,
              }
            : null,
        });

        // Reply to the cast telling the user their share was verified
        try {
          const pointsForVote = 6 + updatedUser.brndPowerLevel * 3;
          const config = getConfig();
          if (config.neynar.apiKey && config.neynar.signerUuid) {
            const replyResponse = await fetch(
              'https://api.neynar.com/v2/farcaster/cast',
              {
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
              },
            );

            if (replyResponse.ok) {
              const replyData = await replyResponse.json();
              console.log(
                '✅ [VerifyShare] Successfully replied to cast:',
                replyData.cast?.hash,
              );
            } else {
              const errorText = await replyResponse.text();
              console.error(
                '❌ [VerifyShare] Failed to reply to cast:',
                replyResponse.status,
                errorText,
              );
            }
          } else {
            console.warn(
              '⚠️ [VerifyShare] Neynar config missing, skipping cast reply',
            );
          }
        } catch (replyError) {
          console.error('❌ [VerifyShare] Error replying to cast:', replyError);
          // Don't fail the request if reply fails
        }

        return hasResponse(res, {
          ...responsePayload,
          castHash,
        });
      } catch (neynarError) {
        console.error(
          '❌ [VerifyShare] Neynar verification failed:',
          neynarError,
        );

        // Handle specific Neynar errors
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
      console.error('❌ [VerifyShare] Unexpected error:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'verifyShare',
        'An unexpected error occurred during verification',
      );
    }
  }

  /**
   * Handles claim retrieval for already shared votes.
   * Looks up existing verified shares by voteId (unix timestamp) and user FID.
   */
  private async handleClaimRetrieval(
    user: QuickAuthPayload,
    voteId: string,
    recipientAddress: string,
    res: Response,
  ): Promise<Response> {
    try {
      // Validate recipient address if provided
      console.log(
        '✅ [ClaimRetrieval] Recipient address:',
        recipientAddress,
        voteId,
      );
      if (recipientAddress && !/^0x[a-fA-F0-9]{40}$/.test(recipientAddress)) {
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Invalid recipient address format',
        );
      }

      // Get the user from database
      const dbUser = await this.userService.getByFid(user.sub);
      console.log('✅ [ClaimRetrieval] DB user:', dbUser);
      if (!dbUser) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'User not found',
        );
      }

      // Get the vote by transactionHash (primary key)
      const vote = await this.brandService.getVoteByTransactionHash(voteId);

      if (!vote) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'Vote not found',
        );
      }

      // Verify vote belongs to user
      if (vote.user.fid !== dbUser.fid) {
        return hasError(
          res,
          HttpStatus.FORBIDDEN,
          'verifyShare',
          'Vote does not belong to user',
        );
      }

      // Get day from vote - use day field if available, otherwise calculate from date
      // Day calculation matches contract: block.timestamp / 86400
      let day: number;
      if (vote.day != null) {
        day = vote.day;
      } else {
        // Calculate day from vote date (same as contract: block.timestamp / 86400)
        const voteTimestamp = Math.floor(new Date(vote.date).getTime() / 1000);
        day = Math.floor(voteTimestamp / 86400);
      }

      console.log('✅ [ClaimRetrieval] Day:', day);

      // Look up existing verified share for this user and day
      const existingShare =
        await this.brandService.getVerifiedShareByUserAndDay(user.sub, day);
      console.log('✅ [ClaimRetrieval] Existing share:', existingShare);

      if (!existingShare) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'No verified share found for this vote.',
        );
      }

      // Note: We don't validate recipient address against user's stored wallet
      // as wallet address is provided by frontend and not stored in user model

      // Generate claim signature if recipient address is provided
      let claimSignature = null;
      console.log('✅ [ClaimRetrieval] Generating claim signature');
      if (recipientAddress) {
        try {
          console.log(
            '✅ [ClaimRetrieval] Generating claim signature for existing share',
          );
          claimSignature = await this.rewardService.generateClaimSignature(
            dbUser.fid,
            day,
            recipientAddress,
            existingShare.castHash,
          );
          console.log(
            '✅ [ClaimRetrieval] Claim signature generated for existing share',
          );
        } catch (claimError) {
          console.error(
            '❌ [ClaimRetrieval] Failed to generate claim signature:',
            claimError,
          );
          // Don't return error here, still return the share info
        }
      }

      // Return response similar to successful verification
      const responsePayload = {
        verified: true,
        pointsAwarded: 0, // Points already awarded when originally shared
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

      console.log('📤 [ClaimRetrieval] Response payload for existing share');

      return hasResponse(res, responsePayload);
    } catch (error) {
      console.error('❌ [ClaimRetrieval] Unexpected error:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'verifyShare',
        'An unexpected error occurred during claim retrieval',
      );
    }
  }

  /**
   * Handles the request to create a new brand.
   *
   * @param {CurrentUser} user - The current user session.
   * @param {{ name: string }} body - An object containing the name of the new brand.
   */
  @Post('/request')
  @UseGuards(AuthorizationGuard)
  async requestBrand(
    @Session() user: CurrentUser,
    @Body() { name }: { name: string },
    @Res() res: Response,
  ): Promise<Response> {
    try {
      console.log(name, user);
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

  /**
   * Handles the request to follow a brand.
   *
   * @param {CurrentUser} user - The current user session.
   * @param {string} id - The ID of the brand to follow.
   */
  @Post('/:id/follow')
  @UseGuards(AuthorizationGuard)
  async followBrand(@Session() user: CurrentUser, @Param('id') id: string) {
    console.log({ user, id });
  }

  // Add these endpoints to your BrandController (brand.controller.ts)
  // Place them after your existing endpoints, before the dev endpoints

  /**
   * DEBUG: Get detailed scoring information for top brands
   * This will help us understand why scores are so similar
   */
  @Get('/debug/scoring')
  async debugScoring(@Res() res: Response) {
    console.log('🔍 [DEBUG] Analyzing scoring system...');

    try {
      const debugInfo = await this.brandService.getDebugScoringInfo();

      return hasResponse(res, debugInfo);
    } catch (error) {
      console.error('❌ Error in debug scoring:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'debugScoring',
        error.message,
      );
    }
  }
  /**
   * Get current cycle rankings with time remaining until cycle end
   * PUBLIC ENDPOINT - No authentication required for easy access
   */
  @Get('/cycles/:period/rankings')
  async getCycleRankings(
    @Param('period') period: 'week' | 'month',
    @Query('limit') limit: number = 10,
    @Res() res: Response,
  ) {
    console.log(`getCycleRankings called - period: ${period}, limit: ${limit}`);

    if (period !== 'week' && period !== 'month') {
      return hasError(
        res,
        HttpStatus.BAD_REQUEST,
        'getCycleRankings',
        'Period must be "week" or "month"',
      );
    }

    try {
      console.log(`Fetching ${period} cycle rankings...`);
      const result = await this.brandService.getCycleRankings(period, limit);
      console.log(`Found ${result.rankings.length} brands for ${period} cycle`);

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
      console.error('Error in getCycleRankings:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getCycleRankings',
        error.message,
      );
    }
  }

  /**
   * Get deployment info and first vote timestamp
   * PUBLIC ENDPOINT - No authentication required for easy access
   */
  @Get('/deployment-info')
  async getDeploymentInfo(@Res() res: Response) {
    console.log('getDeploymentInfo called - public access');

    try {
      const deploymentInfo = await this.brandService.getDeploymentInfo();
      return hasResponse(res, deploymentInfo);
    } catch (error) {
      console.error('Error in getDeploymentInfo:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getDeploymentInfo',
        error.message,
      );
    }
  }

  /**
   * Get historical weekly leaderboard data
   * PUBLIC ENDPOINT - No authentication required for easy access
   *
   * @param {string} week - Week identifier (YYYY-MM-DD format for the Friday of that week)
   * @param {number} limit - Number of brands to return (default: 10)
   * @param {Response} res - The response object
   * @returns {Promise<Response>} Weekly leaderboard data with available weeks picker
   */
  @Get('/weekly-leaderboard')
  async getWeeklyLeaderboard(
    @Query('week') week: string,
    @Query('limit') limit: number = 10,
    @Res() res: Response,
  ) {
    console.log(`getWeeklyLeaderboard called - week: ${week}, limit: ${limit}`);

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
      console.error('Error in getWeeklyLeaderboard:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getWeeklyLeaderboard',
        error.message,
      );
    }
  }

  // ============================================================================
  // LOCAL DEVELOPMENT SEEDING ENDPOINTS
  // ============================================================================

  /**
   * Seeds brands from brands-seed.json file (LOCAL DEVELOPMENT ONLY).
   *
   * Usage:
   * - POST /brand-service/dev/seed (create new brands, skip existing)
   * - POST /brand-service/dev/seed?overwrite=true (update existing brands)
   *
   * @param {string} overwrite - Whether to overwrite existing brands
   * @param {Response} res - The response object
   * @returns {Promise<Response>} Seeding results with statistics
   */
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

  /**
   * Gets database statistics (LOCAL DEVELOPMENT ONLY).
   * Shows brand counts, categories, and distribution.
   *
   * @param {Response} res - The response object
   * @returns {Promise<Response>} Database statistics
   */
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

  /**
   * Previews what would happen during seeding without actually doing it (LOCAL DEVELOPMENT ONLY).
   * Shows which brands would be created, which exist, and which are missing channels.
   *
   * @param {Response} res - The response object
   * @returns {Promise<Response>} Preview results
   */
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

  /**
   * Retrieves recent podiums from all users (public feed).
   * Shows community voting activity with pagination.
   *
   * @param {number} page - Page number for pagination
   * @param {number} limit - Number of podiums per page
   * @param {Response} res - The response object
   * @returns {Promise<Response>} Recent podiums with user and brand details
   */
  @Get('/recent-podiums')
  @UseGuards(AuthorizationGuard)
  async getRecentPodiums(
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 20,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      console.log(
        `🏆 [RecentPodiums] Fetching page ${page} with limit ${limit}`,
      );

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
      console.error('❌ [RecentPodiums] Error fetching podiums:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getRecentPodiums',
        'Failed to fetch recent podiums',
      );
    }
  }

  /**
   * Clears all brands from database (LOCAL DEVELOPMENT ONLY).
   * Use with extreme caution!
   *
   * @param {string} confirm - Must be 'yes' to proceed
   * @param {Response} res - The response object
   * @returns {Promise<Response>} Deletion results
   */
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

  // ============================================================================
  // DEBUG ENDPOINTS FOR TESTING CRON JOBS
  // ============================================================================

  /**
   * Manual trigger for testing cron jobs (ADMIN ONLY).
   * Useful for debugging cron job functionality without waiting for scheduled times.
   */
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
      console.log(`🧪 [TEST] Manual cron job test triggered: ${type}`);

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
      console.error(`❌ [TEST] Cron job test failed:`, error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'testCronJobs',
        `Failed to execute ${type} cron job: ${error.message}`,
      );
    }
  }
}
