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
      castedFrom,
    }: {
      castHash: string;
      voteId: string;
      recipientAddress: string;
      transactionHash: string;
      castedFrom: number;
    },
    @Res() res: Response,
  ): Promise<Response> {
    console.log('=== verifyShare START ===');
    console.log('Input parameters:', {
      userFid: user.sub,
      castHash,
      voteId,
      recipientAddress,
      transactionHash,
      castedFrom,
    });
    try {
      if (!voteId) {
        console.log('❌ Validation failed: Vote ID is required');
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Vote ID is required',
        );
      }
      logger.log(
        'Verifying share for vote id',
        voteId,
        castHash,
        recipientAddress,
        transactionHash,
        castedFrom,
      );

      if (recipientAddress && !/^0x[a-fA-F0-9]{40}$/.test(recipientAddress)) {
        console.log(
          '❌ Validation failed: Invalid recipient address format:',
          recipientAddress,
        );
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'verifyShare',
          'Invalid recipient address format',
        );
      }

      // Validate castHash format if provided
      const hasValidCastHash = castHash && /^0x[a-fA-F0-9]{40}$/.test(castHash);
      console.log(
        'Cast hash provided:',
        hasValidCastHash,
        'castHash:',
        castHash,
      );

      console.log('→ Fetching user from database...');
      const dbUser = await this.userService.getByFid(user.sub);
      if (!dbUser) {
        console.log('❌ User not found for fid:', user.sub);
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'User not found',
        );
      }
      console.log('✓ User found:', {
        userId: dbUser.id,
        fid: dbUser.fid,
        points: dbUser.points,
      });

      console.log('→ Fetching vote by transaction hash:', transactionHash);
      const vote = await this.brandService.getVoteByTransactionHash(
        transactionHash as string,
      );
      logger.log('THE VOTE on the verify share IS', vote);
      console.log(
        'Vote data:',
        vote
          ? {
              transactionHash: vote.transactionHash,
              userId: vote.user?.fid,
              shared: vote.shared,
              date: vote.date,
            }
          : null,
      );

      if (!vote) {
        console.log('❌ Vote not found for transaction hash:', transactionHash);
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'Vote not found',
        );
      }

      if (vote.user.fid !== dbUser.fid) {
        console.log('❌ Vote ownership mismatch:', {
          voteUserFid: vote.user.fid,
          dbUserFid: dbUser.fid,
        });
        return hasError(
          res,
          HttpStatus.FORBIDDEN,
          'verifyShare',
          'Vote does not belong to user',
        );
      }

      // If vote is already shared, skip verification and just return claim signature
      if (vote.shared) {
        console.log(
          '→ Vote has already been shared, skipping verification and generating claim signature only',
        );

        if (!vote.castHash) {
          console.log('❌ Vote is marked as shared but castHash is missing');
          return hasError(
            res,
            HttpStatus.INTERNAL_SERVER_ERROR,
            'verifyShare',
            'Vote is marked as shared but cast hash is missing',
          );
        }

        // Calculate day from vote date (or use stored day if available)
        const voteTimestamp = Math.floor(new Date(vote.date).getTime() / 1000);
        const day = vote.day || Math.floor(voteTimestamp / 86400);
        console.log('Using stored castHash:', vote.castHash);
        console.log('Calculated day:', day, 'from vote date:', vote.date);

        // Generate claim signature if recipient address is provided
        let claimSignature = null;
        if (recipientAddress) {
          console.log(
            '→ Generating claim signature for recipient:',
            recipientAddress,
          );
          try {
            claimSignature = await this.rewardService.generateClaimSignature(
              dbUser.fid,
              day,
              recipientAddress,
              vote.castHash,
            );
            console.log('✓ Claim signature generated:', {
              amount: claimSignature.amount,
              deadline: claimSignature.deadline,
              canClaim: claimSignature.canClaim,
            });
          } catch (claimError) {
            console.error('❌ Error generating claim signature:', claimError);
            // Do nothing, skip claim sig on error
          }
        } else {
          console.log(
            'No recipient address provided, skipping claim signature generation',
          );
        }

        const responsePayload = {
          verified: true,
          pointsAwarded: 0, // No points awarded since already shared
          newTotalPoints: dbUser.points, // Return current points without adding
          message: 'Vote was already shared. Claim signature generated.',
          day,
          castHash: vote.castHash,
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

        console.log('=== verifyShare SUCCESS (already shared) ===');
        return hasResponse(res, {
          ...responsePayload,
        });
      }

      // Vote is not shared yet, proceed with full verification
      console.log('→ Vote not yet shared, proceeding with full verification');
      try {
        console.log('→ Initializing Neynar service...');
        const neynar = new NeynarService();
        let resolvedCastHash = castHash;
        let castData;

        // If castHash is provided and valid, fetch cast directly
        // Otherwise, poll for the cast
        if (hasValidCastHash) {
          console.log(
            '→ Cast hash provided - fetching cast by hash:',
            castHash,
          );
          castData = await neynar.getCastByHash(castHash);
          console.log('✓ Cast data retrieved:', {
            hash: castData.hash,
            authorFid: castData.author?.fid,
          });
        } else {
          console.log('→ Cast hash not provided - starting cast polling...');
          const validEmbedUrls = [
            'https://brnd.land',
            'https://rebrnd.lat',
            'https://www.brnd.land',
            'https://poiesis.anky.app',
            'https://brnd-v2-backend-production.up.railway.app',
          ];

          const expectedTxHash = vote.transactionHash;
          console.log('Expected transaction hash:', expectedTxHash);

          // Calculate the vote's day (BRND world day)
          const voteTimestamp = Math.floor(
            new Date(vote.date).getTime() / 1000,
          );
          const voteDay = Math.floor(voteTimestamp / 86400);
          console.log(
            'Vote day (BRND world):',
            voteDay,
            'from vote date:',
            vote.date,
          );

          let foundCast = null;
          const maxPollAttempts = 10;
          const pollInterval = 3000; // 3 seconds

          // Poll for the cast
          for (let attempt = 0; attempt < maxPollAttempts; attempt++) {
            console.log(`Polling attempt ${attempt + 1}/${maxPollAttempts}`);
            try {
              console.log('Fetching user casts for fid:', user.sub);
              const casts = await neynar.getUserCasts(user.sub, 10, false);
              console.log(`Retrieved ${casts.length} casts`);

              for (const cast of casts) {
                // Check if cast is from the same BRND world day as the vote
                // Cast timestamp is in ISO format (e.g., "2025-12-22T14:58:08.000Z")
                const castTimestampSeconds = Math.floor(
                  new Date(cast.timestamp).getTime() / 1000,
                );
                const castDay = Math.floor(castTimestampSeconds / 86400);

                if (castDay !== voteDay) {
                  console.log(
                    `Cast ${cast.hash} is from a different day (cast day: ${castDay}, vote day: ${voteDay}, timestamp: ${cast.timestamp})`,
                  );
                  continue; // Cast is from a different day
                }

                console.log(
                  `Checking cast ${cast.hash} (day: ${castDay}, timestamp: ${cast.timestamp}, embeds: ${cast.embeds?.length || 0})`,
                );

                // Check if cast has the correct embed URL with the transaction hash
                for (const embed of cast.embeds) {
                  if ('url' in embed) {
                    const embedUrl = embed.url as string;
                    const hasValidBaseUrl = validEmbedUrls.some((baseUrl) =>
                      embedUrl.includes(baseUrl),
                    );

                    console.log(
                      `  Embed URL: ${embedUrl}, hasValidBaseUrl: ${hasValidBaseUrl}`,
                    );

                    if (hasValidBaseUrl && embedUrl.includes('/podium/')) {
                      // Extract transaction hash from URL (handle query params/fragments)
                      const txHashFromEmbed = embedUrl
                        .split('/podium/')[1]
                        ?.split('?')[0]
                        ?.split('#')[0]
                        ?.trim();
                      console.log(
                        `  Extracted tx hash from embed: ${txHashFromEmbed}, expected: ${expectedTxHash}`,
                      );
                      if (txHashFromEmbed === expectedTxHash) {
                        foundCast = cast;
                        resolvedCastHash = cast.hash;
                        console.log(
                          `✓ Found matching cast! Hash: ${resolvedCastHash}`,
                        );
                        break;
                      }
                    }
                  }
                }

                if (foundCast) {
                  break;
                }
              }

              if (foundCast) {
                break;
              }

              // Wait before next poll attempt
              if (attempt < maxPollAttempts - 1) {
                console.log(`Waiting ${pollInterval}ms before next attempt...`);
                await new Promise((resolve) =>
                  setTimeout(resolve, pollInterval),
                );
              }
            } catch (pollError) {
              console.error(
                `Error on polling attempt ${attempt + 1}:`,
                pollError,
              );
              logger.error('Error polling for cast:', pollError);
              // Continue to next attempt
            }
          }

          if (!foundCast) {
            console.log('❌ Could not find cast after all polling attempts');
            return hasError(
              res,
              HttpStatus.NOT_FOUND,
              'verifyShare',
              'Could not find cast with the expected embed URL and transaction hash',
            );
          }

          castData = foundCast;
          console.log('✓ Cast data retrieved from polling:', {
            hash: castData.hash,
            authorFid: castData.author?.fid,
          });
        }

        console.log('→ Validating cast author...');
        console.log(
          'Cast author fid:',
          castData.author.fid,
          'User fid:',
          user.sub,
        );
        if (castData.author.fid !== user.sub) {
          console.log('❌ Cast author mismatch');
          return hasError(
            res,
            HttpStatus.FORBIDDEN,
            'verifyShare',
            'Cast was not posted by the authenticated user',
          );
        }
        console.log('✓ Cast author validated');

        const validEmbedUrls = [
          'https://brnd.land',
          'https://rebrnd.lat',
          'https://www.brnd.land',
          'https://poiesis.anky.app',
          'https://brnd-v2-backend-production.up.railway.app',
        ];

        console.log('→ Validating embed URLs...');
        console.log(
          'Cast embeds:',
          castData.embeds.map((e: any) => e.url || 'no url'),
        );
        const correctEmbedIndex = castData.embeds.findIndex((embed) => {
          if ('url' in embed) {
            return validEmbedUrls.some((baseUrl) =>
              embed.url.includes(baseUrl),
            );
          }
          return false;
        });

        if (correctEmbedIndex === -1) {
          console.log('❌ No valid embed URL found in cast');
          return hasError(
            res,
            HttpStatus.BAD_REQUEST,
            'verifyShare',
            'Cast does not contain the correct embed URL',
          );
        }
        console.log(`✓ Found valid embed at index ${correctEmbedIndex}`);

        const correctEmbed = castData.embeds[correctEmbedIndex] as any;
        const correctEmbedUrl = correctEmbed.url;
        const transactionHashFromQueryParam =
          correctEmbedUrl.split('/podium/')[1];
        console.log('Embed URL:', correctEmbedUrl);
        console.log(
          'Transaction hash from embed:',
          transactionHashFromQueryParam,
        );
        console.log('Vote transaction hash:', vote.transactionHash);

        if (vote.transactionHash !== transactionHashFromQueryParam) {
          console.log('❌ Transaction hash mismatch');
          return hasError(
            res,
            HttpStatus.BAD_REQUEST,
            'verifyShare',
            'Cast does not contain the correct tx hash',
          );
        }
        console.log('✓ Transaction hash validated');

        await this.brandService.markVoteAsShared(
          vote.transactionHash,
          resolvedCastHash,
        );

        // Triple-check: Re-fetch vote to verify it was marked as shared
        // This prevents race conditions where two requests might process simultaneously
        const updatedVote = await this.brandService.getVoteByTransactionHash(
          vote.transactionHash,
        );
        
        // If vote is not shared after our update, something went wrong
        if (!updatedVote?.shared) {
          return hasError(
            res,
            HttpStatus.INTERNAL_SERVER_ERROR,
            'verifyShare',
            'Failed to mark vote as shared',
          );
        }

        // If vote was already shared when we started (race condition - another request got there first),
        // skip points and just return claim signature
        if (vote.shared) {
          const voteTimestamp = Math.floor(new Date(vote.date).getTime() / 1000);
          const day = Math.floor(voteTimestamp / 86400);
          
          let claimSignature = null;
          if (recipientAddress) {
            try {
              claimSignature = await this.rewardService.generateClaimSignature(
                dbUser.fid,
                day,
                recipientAddress,
                resolvedCastHash,
              );
            } catch (claimError) {
              // Skip on error
            }
          }

          return hasResponse(res, {
            verified: true,
            pointsAwarded: 0,
            newTotalPoints: dbUser.points,
            message: 'Vote was already shared. Claim signature generated.',
            day,
            castHash: resolvedCastHash,
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
          });
        }

        const updatedUser = await this.userService.addPoints(dbUser.id, 3);

        const voteTimestamp = Math.floor(new Date(vote.date).getTime() / 1000);
        const day = Math.floor(voteTimestamp / 86400);
        console.log('Calculated day:', day, 'from vote date:', vote.date);

        console.log('→ Verifying share for reward...');
        await this.rewardService.verifyShareForReward(
          dbUser.fid,
          day,
          resolvedCastHash,
        );
        console.log('✓ Share verified for reward');

        let claimSignature = null;
        if (recipientAddress) {
          console.log(
            '→ Generating claim signature for recipient:',
            recipientAddress,
          );
          try {
            claimSignature = await this.rewardService.generateClaimSignature(
              dbUser.fid,
              day,
              recipientAddress,
              resolvedCastHash,
            );
            console.log('✓ Claim signature generated:', {
              amount: claimSignature.amount,
              deadline: claimSignature.deadline,
              canClaim: claimSignature.canClaim,
            });
          } catch (claimError) {
            console.error('❌ Error generating claim signature:', claimError);
            // Do nothing, skip claim sig on error
          }
        } else {
          console.log(
            'No recipient address provided, skipping claim signature generation',
          );
        }

        const responsePayload = {
          verified: true,
          pointsAwarded: 3,
          newTotalPoints: updatedUser.points,
          message: 'Share verified successfully! 3 points awarded.',
          day,
          castHash: resolvedCastHash,
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

        console.log('→ Preparing response payload:', {
          verified: responsePayload.verified,
          pointsAwarded: responsePayload.pointsAwarded,
          newTotalPoints: responsePayload.newTotalPoints,
          day: responsePayload.day,
          castHash: responsePayload.castHash,
          hasClaimSignature: !!responsePayload.claimSignature,
        });

        try {
          console.log('→ Attempting to post reply cast...');
          const pointsForVote = 6 + updatedUser.brndPowerLevel * 3;
          const config = getConfig();
          if (config.neynar.apiKey && config.neynar.signerUuid) {
            const replyText = `Thank you for voting @${castData.author.username}. Your vote has been verified. You earned ${pointsForVote} points and now have a total of ${updatedUser.points} points.\n\nYou can now claim ${vote.brndPaidWhenCreatingPodium * 10} $BRND on the miniapp.`;
            console.log('Reply text:', replyText);
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
                    {
                      cast_id: {
                        hash: resolvedCastHash,
                        fid: castData.author.fid,
                      },
                    },
                  ],

                  text: replyText,
                }),
              },
            );
            console.log(
              '✓ Reply cast posted successfully. Status:',
              replyResponse.status,
            );
          } else {
            console.log('⚠ Neynar config missing, skipping reply cast');
          }
        } catch (replyError) {
          console.error('❌ Error posting reply cast:', replyError);
          // Do nothing if reply failed
        }

        console.log('=== verifyShare SUCCESS ===');
        return hasResponse(res, {
          ...responsePayload,
        });
      } catch (neynarError) {
        console.error('❌ Neynar error:', neynarError);
        console.error('Error message:', neynarError.message);
        console.error('Error stack:', neynarError.stack);
        if (neynarError.message?.includes('Cast not found')) {
          console.log('→ Cast not found error detected');
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
      console.error('❌ Unexpected error in verifyShare:', error);
      console.error('Error message:', error.message);
      console.error('Error stack:', error.stack);
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
      console.log('→ Handling claim retrieval flow');
      console.log('→ Fetching user from database...');
      console.log('User fid:', user.sub);

      const dbUser = await this.userService.getByFid(user.sub);
      if (!dbUser) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'verifyShare',
          'User not found',
        );
      }
      console.log('✓ User found:', {
        userId: dbUser.id,
        fid: dbUser.fid,
        points: dbUser.points,
      });

      const vote = await this.brandService.getVoteByTransactionHash(voteId);

      console.log('→ fetched vote by transaction hash:', vote);
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
      console.log('→ existing share:', existingShare);

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
}
