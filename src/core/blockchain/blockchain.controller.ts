import {
  Body,
  Controller,
  Get,
  Logger,
  Param,
  Post,
  UseGuards,
} from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';

import { BlockchainService } from './services/blockchain.service';
import { PowerLevelService } from './services/power-level.service';
import { SignatureService } from './services/signature.service';
import { RewardService } from './services/reward.service';
import { CastVerificationService } from './services/cast-verification.service';
import { IndexerService } from './services/indexer.service';
import { PodiumService } from './services/podium.service';

import {
  AuthorizationGuard,
  QuickAuthPayload,
  IndexerGuard,
} from '../../security/guards';
import { Session } from '../../security/decorators';

import { HttpStatus } from '../../utils';
import {
  BadRequestException,
  ForbiddenException,
  InternalServerErrorException,
} from '@nestjs/common';

import { logger } from '../../main';
import {
  SubmitVoteDto,
  SubmitBrandDto,
  SubmitRewardClaimDto,
  UpdateUserLevelDto,
  ClaimPodiumSignatureDto,
  BuyPodiumSignatureDto,
  ClaimFeesSignatureDto,
  CollectibleMintedDto,
  CollectibleBoughtDto,
} from './dto';
import { BlockchainBrandDto, PrepareMetadataDto } from '../admin/dto';
import { AdminService } from '../admin/services/admin.service';

@ApiTags('blockchain-service')
@Controller('blockchain-service')
export class BlockchainController {
  constructor(
    private readonly blockchainService: BlockchainService,
    private readonly powerLevelService: PowerLevelService,
    private readonly signatureService: SignatureService,
    private readonly rewardService: RewardService,
    private readonly castVerificationService: CastVerificationService,
    private readonly indexerService: IndexerService,
    private readonly adminService: AdminService,
    private readonly podiumService: PodiumService,
  ) {}

  @Post('/authorize-wallet')
  @UseGuards(AuthorizationGuard)
  async authorizeWallet(
    @Session() session: QuickAuthPayload,
    @Body() body: { walletAddress: string; deadline: number },
  ) {
    try {
      logger.log(
        `🔐 [BLOCKCHAIN] Authorization signature request for FID: ${session.sub}`,
      );

      const { walletAddress, deadline } = body;

      if (!walletAddress || !deadline) {
        throw new BadRequestException(
          'Wallet address and deadline are required',
        );
      }

      const authData =
        await this.signatureService.generateAuthorizationSignature(
          session.sub,
          walletAddress,
          deadline,
        );

      Logger.log('THE AUTH DATA IS', authData, walletAddress, session.sub);

      return {
        authData,
        fid: session.sub,
        walletAddress,
        deadline,
      };
    } catch (error) {
      logger.error('Failed to generate authorization signature:', error);
      throw new Error('Failed to generate authorization signature');
    }
  }

  @Post('/level-up')
  @UseGuards(AuthorizationGuard)
  async levelUp(
    @Session() session: QuickAuthPayload,
    @Body() body: { newLevel: number; deadline: number; walletAddress: string },
  ) {
    try {
      logger.log(
        `📈 [BLOCKCHAIN] Level up signature request for FID: ${session.sub}`,
      );

      const { newLevel, deadline, walletAddress } = body;

      if (!newLevel || !deadline || !walletAddress) {
        throw new BadRequestException(
          'New level, deadline, and wallet address are required',
        );
      }

      const canLevelUp = await this.powerLevelService.canLevelUp(
        session.sub,
        newLevel,
      );

      if (!canLevelUp.eligible) {
        // Build a user-friendly error message
        let errorMessage = 'Cannot level up';

        // Handle specific error cases with detailed messages
        if (canLevelUp.reason === 'Can only level up one level at a time') {
          const nextLevel = canLevelUp.currentLevel + 1;
          const unmetRequirements = canLevelUp.requirements.filter(
            (req) => !req.met,
          );

          if (unmetRequirements.length > 0) {
            const reqMessages = unmetRequirements.map((req) => {
              if (req.current !== undefined && req.required !== undefined) {
                return `${req.description} (You have: ${req.current.toLocaleString()}, Required: ${req.required.toLocaleString()})`;
              }
              return req.description;
            });
            errorMessage = `You can only level up one level at a time. Your current level is ${canLevelUp.currentLevel}, and you're trying to reach level ${canLevelUp.nextLevel}. To level up to level ${nextLevel}, you need:\n${reqMessages.join('\n')}`;
          } else {
            errorMessage = `You can only level up one level at a time. Your current level is ${canLevelUp.currentLevel}, and you're trying to reach level ${canLevelUp.nextLevel}. Please level up to level ${nextLevel} first.`;
          }
        } else if (
          canLevelUp.reason === 'Target level must be higher than current level'
        ) {
          errorMessage = `Target level (${canLevelUp.nextLevel}) must be higher than your current level (${canLevelUp.currentLevel}).`;
        } else if (canLevelUp.reason === 'Invalid target level') {
          errorMessage = `Invalid target level: ${canLevelUp.nextLevel}`;
        } else if (canLevelUp.reason === 'Requirements not met') {
          // Build detailed message from requirements
          const unmetRequirements = canLevelUp.requirements.filter(
            (req) => !req.met,
          );
          if (unmetRequirements.length > 0) {
            const reqMessages = unmetRequirements.map((req) => {
              if (req.current !== undefined && req.required !== undefined) {
                return `${req.description} (You have: ${req.current.toLocaleString()}, Required: ${req.required.toLocaleString()})`;
              }
              return req.description;
            });
            errorMessage = `Requirements not met for level ${canLevelUp.nextLevel}:\n${reqMessages.join('\n')}`;
          } else {
            errorMessage = `Requirements not met for level ${canLevelUp.nextLevel}`;
          }
        } else if (canLevelUp.reason) {
          errorMessage = `${errorMessage}: ${canLevelUp.reason}`;
        }

        console.log('errorMessage', errorMessage);
        console.log('canLevelUp', canLevelUp);
        console.log('SENDING FORBIDDEN ERROR');

        // Throw ForbiddenException with structured error response
        const errorResponse = {
          message: errorMessage,
          error: 'Level Up Validation Failed',
          statusCode: 403,
          validation: {
            eligible: canLevelUp.eligible,
            reason: canLevelUp.reason,
            currentLevel: canLevelUp.currentLevel,
            targetLevel: canLevelUp.nextLevel,
            nextLevel: canLevelUp.currentLevel + 1, // The level they should be targeting
            requirements: canLevelUp.requirements,
          },
        };

        throw new ForbiddenException(errorResponse);
      }

      const signature = await this.signatureService.generateLevelUpSignature(
        session.sub,
        newLevel,
        deadline,
        walletAddress,
      );

      return {
        signature,
        fid: session.sub,
        newLevel,
        deadline,
        walletAddress,
        validation: canLevelUp,
      };
    } catch (error) {
      // If it's already a ForbiddenException with our structured response, re-throw it
      if (error instanceof ForbiddenException) {
        throw error;
      }

      // If it's a BadRequestException, re-throw it
      if (error instanceof BadRequestException) {
        throw error;
      }

      // For other errors, log and throw generic error
      logger.error('Failed to generate level up signature:', error);
      throw new InternalServerErrorException(
        error.message || 'Failed to generate level up signature',
      );
    }
  }

  @Post('/claim-reward')
  @UseGuards(AuthorizationGuard)
  async claimReward(
    @Session() session: QuickAuthPayload,
    @Body() body: { day: number; recipientAddress: string },
  ) {
    try {
      logger.log(
        `💰 [BLOCKCHAIN] Reward claim signature request for FID: ${session.sub}`,
      );

      const { day, recipientAddress } = body;

      if (day === undefined || !recipientAddress) {
        throw new BadRequestException('Day and recipient address are required');
      }

      const claimResponse = await this.rewardService.generateClaimSignature(
        session.sub,
        day,
        recipientAddress,
      );

      return claimResponse;
    } catch (error) {
      logger.error('Failed to generate reward claim signature:', error);
      throw new InternalServerErrorException(
        error.message || 'Failed to generate reward claim signature',
      );
    }
  }

  @Get('/power-level/:fid')
  @UseGuards(AuthorizationGuard)
  async getPowerLevel(
    @Session() session: QuickAuthPayload,
    @Param('fid') fid: string,
  ) {
    try {
      if (session.sub.toString() !== fid) {
        throw new ForbiddenException('Can only check your own power level');
      }

      const powerLevelData = await this.powerLevelService.getUserPowerLevel(
        parseInt(fid),
      );

      return powerLevelData;
    } catch (error) {
      logger.error('Failed to get power level:', error);
      throw new InternalServerErrorException('Failed to get power level data');
    }
  }

  @Get('/user-stake/:fid')
  @UseGuards(AuthorizationGuard)
  async getUserStake(
    @Session() session: QuickAuthPayload,
    @Param('fid') fid: string,
  ) {
    try {
      if (session.sub.toString() !== fid) {
        throw new ForbiddenException('Can only check your own stake');
      }

      const stakeData = await this.blockchainService.getUserStakeInfo(
        parseInt(fid),
      );

      return stakeData;
    } catch (error) {
      logger.error('Failed to get user stake:', error);
      throw new InternalServerErrorException('Failed to get stake information');
    }
  }

  @Get('/claim-status/:day')
  @UseGuards(AuthorizationGuard)
  async getClaimStatus(
    @Session() session: QuickAuthPayload,
    @Param('day') day: string,
  ) {
    try {
      logger.log(
        `🔍 [BLOCKCHAIN] Claim status request for FID: ${session.sub}, Day: ${day}`,
      );

      const claimStatus = await this.rewardService.getClaimStatus(
        session.sub,
        parseInt(day),
      );

      return claimStatus;
    } catch (error) {
      logger.error('Failed to get claim status:', error);
      throw new InternalServerErrorException('Failed to get claim status');
    }
  }

  @Get('/share-status/:day')
  @UseGuards(AuthorizationGuard)
  async getShareStatus(
    @Session() session: QuickAuthPayload,
    @Param('day') day: string,
  ) {
    try {
      const shareStatus = await this.castVerificationService.getShareStatus(
        session.sub,
        parseInt(day),
      );

      return shareStatus;
    } catch (error) {
      logger.error('Failed to get share status:', error);
      throw new InternalServerErrorException('Failed to get share status');
    }
  }

  @Post('/authorize-vote')
  @UseGuards(AuthorizationGuard)
  async authorizeVote(
    @Session() session: QuickAuthPayload,
    @Body()
    body: {
      walletAddress: string;
      brandIds: [number, number, number];
      deadline: number;
    },
  ) {
    try {
      logger.log(
        `🗳️ [BLOCKCHAIN] Vote authorization signature request for FID: ${session.sub}`,
      );

      const { walletAddress, brandIds, deadline } = body;

      if (!walletAddress || !brandIds || brandIds.length !== 3 || !deadline) {
        throw new BadRequestException(
          'Wallet address, exactly 3 brand IDs, and deadline are required',
        );
      }

      // Validate brand IDs exist (optional additional validation)
      // You could add brand validation here if needed

      // Generate the same authorization signature as /authorize-wallet
      // This is the same signature format needed for vote authorization
      const authData =
        await this.signatureService.generateAuthorizationSignature(
          session.sub,
          walletAddress,
          deadline,
        );

      return {
        authData,
        fid: session.sub,
        walletAddress,
        brandIds,
        deadline,
        message: 'Authorization data generated for voting',
      };
    } catch (error) {
      logger.error('Failed to generate vote authorization signature:', error);
      throw new Error('Failed to generate vote authorization signature');
    }
  }

  @Get('/user-rewards')
  @UseGuards(AuthorizationGuard)
  async getUserRewards(@Session() session: QuickAuthPayload) {
    try {
      logger.log(
        `📊 [BLOCKCHAIN] User rewards request for FID: ${session.sub}`,
      );

      const rewardHistory = await this.rewardService.getUserRewardHistory(
        session.sub,
      );

      return rewardHistory;
    } catch (error) {
      logger.error('Failed to get user rewards:', error);
      throw new InternalServerErrorException('Failed to get user rewards');
    }
  }

  @Get('/user-info/:walletAddress')
  async getUserInfo(@Param('walletAddress') walletAddress: string) {
    try {
      logger.log(
        `🔍 [BLOCKCHAIN] User info request for wallet: ${walletAddress}`,
      );

      // This would typically query the smart contract for user info
      // For now, return mock data structure matching V3 contract
      const mockUserInfo = {
        fid: 0,
        brndPowerLevel: 1,
        lastVoteDay: 0,
        totalVotes: 0,
        expectedReward: '1000000000000000000000', // 1000 BRND
        hasVotedToday: false,
        hasSharedToday: false,
        canClaimToday: false,
      };

      return mockUserInfo;
    } catch (error) {
      logger.error('Failed to get user info:', error);
      throw new InternalServerErrorException('Failed to get user info');
    }
  }

  @Post('/webhooks/farcaster/cast-created')
  async handleCastWebhook(@Body() payload: any) {
    try {
      logger.log(`📱 [WEBHOOK] Received cast webhook`);

      const processed =
        await this.castVerificationService.processCastWebhook(payload);

      return { processed };
    } catch (error) {
      logger.error('Failed to process cast webhook:', error);
      throw new InternalServerErrorException('Failed to process cast webhook');
    }
  }

  /**
   * Handles collectible mint events from Ponder indexer
   * Updates all votes with matching brand combination to mark them as collectibles
   */
  @Post('/collectible-minted')
  @UseGuards(IndexerGuard)
  async collectibleMinted(@Body() data: CollectibleMintedDto) {
    try {
      logger.log(
        `🏆 [INDEXER] Received collectible mint: Token #${data.tokenId}, Brands: [${data.brandIds.join(', ')}]`,
      );

      const result = await this.podiumService.handleCollectibleMinted(data);

      return {
        success: true,
        message: 'Collectible mint processed successfully',
        tokenId: data.tokenId,
        affected: result.affected,
      };
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Failed to process collectible mint for Token #${data.tokenId}:`,
        error,
      );
      throw new InternalServerErrorException(
        `Failed to process collectible mint: ${error.message}`,
      );
    }
  }

  /**
   * Handles collectible buy events from Ponder indexer
   * Updates all votes with matching tokenId to reflect new ownership
   */
  @Post('/collectible-bought')
  @UseGuards(IndexerGuard)
  async collectibleBought(@Body() data: CollectibleBoughtDto) {
    try {
      logger.log(
        `💰 [INDEXER] Received collectible buy: Token #${data.tokenId}, New Owner: ${data.newOwnerFid}`,
      );

      const result = await this.podiumService.handleCollectibleBought(data);

      return {
        success: true,
        message: 'Collectible buy processed successfully',
        tokenId: data.tokenId,
        newOwnerFid: data.newOwnerFid,
        affected: result.affected,
      };
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Failed to process collectible buy for Token #${data.tokenId}:`,
        error,
      );
      throw new InternalServerErrorException(
        `Failed to process collectible buy: ${error.message}`,
      );
    }
  }

  /**
   * Handles vote submissions from Ponder indexer
   */
  @Post('/submit-vote')
  @UseGuards(IndexerGuard)
  async submitVote(@Body() submitVoteDto: SubmitVoteDto) {
    try {
      logger.log(`🗳️ [INDEXER] Received vote submission: ${submitVoteDto.id}`);

      await this.indexerService.handleVoteSubmission(submitVoteDto);

      return {
        success: true,
        message: 'Vote processed successfully',
        voteId: submitVoteDto.id,
      };
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Failed to process vote ${submitVoteDto.id}:`,
        error,
      );
      throw new InternalServerErrorException(
        `Failed to process vote: ${error.message}`,
      );
    }
  }

  /**
   * Handles reward claim submissions from Ponder indexer
   */
  @Post('/submit-reward-claim')
  @UseGuards(IndexerGuard)
  async submitRewardClaim(@Body() submitRewardClaimDto: SubmitRewardClaimDto) {
    try {
      logger.log(
        `💰 [INDEXER] Received reward claim submission: ${submitRewardClaimDto.id}`,
      );

      await this.indexerService.handleRewardClaimSubmission(
        submitRewardClaimDto,
      );

      return {
        success: true,
        message: 'Reward claim processed successfully',
        claimId: submitRewardClaimDto.id,
      };
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Failed to process reward claim ${submitRewardClaimDto.id}:`,
        error,
      );
      throw new InternalServerErrorException(
        `Failed to process reward claim: ${error.message}`,
      );
    }
  }

  /**
   * Handles user level-up updates from Ponder indexer
   */
  @Post('/update-user-level')
  @UseGuards(IndexerGuard)
  async updateUserLevel(@Body() updateUserLevelDto: UpdateUserLevelDto) {
    try {
      logger.log(
        `📈 [INDEXER] Received user level update: ${updateUserLevelDto.levelUpId}`,
      );

      await this.indexerService.handleUserLevelUpdate(updateUserLevelDto);

      return {
        success: true,
        message: 'User level update processed successfully',
        levelUpId: updateUserLevelDto.levelUpId,
        fid: updateUserLevelDto.fid,
        newLevel: updateUserLevelDto.brndPowerLevel,
      };
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Failed to process user level update ${updateUserLevelDto.levelUpId}:`,
        error,
      );
      throw new InternalServerErrorException(
        `Failed to process user level update: ${error.message}`,
      );
    }
  }

  /**
   * Handles brand creation from Ponder indexer
   * Creates brand in database based on blockchain data
   */
  @Post('/brands')
  @UseGuards(IndexerGuard)
  async createOrUpdateBrandFromBlockchain(
    @Body() blockchainBrandDto: BlockchainBrandDto,
  ) {
    try {
      logger.log(
        `📋 [INDEXER] Received brand creation: ${blockchainBrandDto.id} - ${blockchainBrandDto.handle}`,
      );

      const brand =
        await this.adminService.createOrUpdateBrandFromBlockchain(
          blockchainBrandDto,
        );

      return {
        success: true,
        message: 'Brand created successfully from blockchain',
        brandId: brand.id,
        onChainId: brand.onChainId,
        handle: brand.onChainHandle,
        fid: brand.onChainFid,
      };
    } catch (error) {
      logger.error(
        `❌ [INDEXER] Failed to create brand ${blockchainBrandDto.id}:`,
        error,
      );
      throw new InternalServerErrorException(
        `Failed to create brand from blockchain: ${error.message}`,
      );
    }
  }

  /**
   * Prepares brand metadata for on-chain creation
   * Validates data first, then uploads metadata to IPFS and returns hash for smart contract
   * Protected by IndexerGuard (API key authentication)
   */
  @Post('brands/prepare-metadata')
  @UseGuards(IndexerGuard)
  async prepareBrandMetadata(@Body() prepareMetadataDto: PrepareMetadataDto) {
    try {
      logger.log(
        `📋 [INDEXER] Received brand metadata preparation request for: ${prepareMetadataDto.handle}`,
      );

      // Step 1: Validate brand data first
      logger.log('Validating brand data...');
      const validation =
        await this.adminService.validateBrandForCreation(prepareMetadataDto);

      if (!validation.valid) {
        logger.log('Brand validation failed:', validation);
        return {
          success: false,
          valid: false,
          message: validation.message,
          conflicts: validation.conflicts || [],
        };
      }

      logger.log(
        'Brand validation passed, proceeding to metadata preparation...',
      );

      // Step 2: Prepare metadata and upload to IPFS
      const result =
        await this.adminService.prepareBrandMetadata(prepareMetadataDto);
      logger.log('Brand metadata prepared successfully:', result);

      return {
        success: true,
        valid: true,
        metadataHash: result.metadataHash,
        handle: result.handle,
        fid: result.fid,
        walletAddress: result.walletAddress,
        message:
          'Brand validation passed and metadata uploaded to IPFS. Use the metadataHash to create the brand on-chain.',
      };
    } catch (error) {
      logger.error('Error in prepareBrandMetadata:', error);
      throw new InternalServerErrorException(
        error.message || 'Failed to prepare brand metadata',
      );
    }
  }

  /**
   * Claim Podium Signature
   * Generates an EIP-712 signature authorizing a user to claim (mint) a new podium NFT
   */
  @Post('/podium/claim-signature')
  @UseGuards(AuthorizationGuard)
  async claimPodiumSignature(
    @Session() session: QuickAuthPayload,
    @Body() body: ClaimPodiumSignatureDto,
  ) {
    try {
      logger.log(
        `🏆 [PODIUM] Claim signature request for FID: ${session.sub}, Brands: [${body.brandIds.join(', ')}]`,
      );

      const { walletAddress, brandIds, deadline } = body;

      // Validate deadline (not more than 24 hours in the future)
      const maxDeadline = Math.floor(Date.now() / 1000) + 24 * 60 * 60;
      if (deadline > maxDeadline) {
        throw new BadRequestException(
          'Deadline cannot be more than 24 hours in the future',
        );
      }

      // Check eligibility
      const eligibility = await this.podiumService.checkClaimEligibility(
        session.sub,
        brandIds,
      );

      if (!eligibility.eligible) {
        console.log('🔑 [PODIUM SIGNATURE] Not eligible:', eligibility.reason);
        throw new ForbiddenException({
          error: 'NOT_ELIGIBLE',
          message:
            eligibility.reason || 'User not eligible to claim this podium',
        });
      }

      console.log('🔑 [PODIUM SIGNATURE] Signer address:', walletAddress);
      console.log('🔑 [PODIUM SIGNATURE] FID:', session.sub);
      console.log('🔑 [PODIUM SIGNATURE] Brands:', brandIds);
      console.log('🔑 [PODIUM SIGNATURE] Deadline:', deadline);
      // Generate signature
      const signature =
        await this.signatureService.generateClaimPodiumSignature(
          session.sub,
          walletAddress,
          brandIds,
          deadline,
        );

      const BASE_PRICE = '1000000000000000000000000'; // 1,000,000 BRND

      return {
        signature,
        price: BASE_PRICE,
        eligible: true,
        reason: null,
      };
    } catch (error) {
      if (
        error instanceof BadRequestException ||
        error instanceof ForbiddenException
      ) {
        throw error;
      }

      logger.error('Failed to generate claim podium signature:', error);
      throw new InternalServerErrorException(
        error.message || 'Failed to generate claim podium signature',
      );
    }
  }

  /**
   * Buy Podium Signature
   * Generates an EIP-712 signature authorizing a user to buy an existing podium NFT
   */
  @Post('/podium/buy-signature')
  @UseGuards(AuthorizationGuard)
  async buyPodiumSignature(
    @Session() session: QuickAuthPayload,
    @Body() body: BuyPodiumSignatureDto,
  ) {
    try {
      logger.log(
        `💰 [PODIUM] Buy signature request for FID: ${session.sub}, TokenId: ${body.tokenId}`,
      );

      const { walletAddress, tokenId, deadline } = body;

      // Validate deadline
      const maxDeadline = Math.floor(Date.now() / 1000) + 24 * 60 * 60;
      if (deadline > maxDeadline) {
        throw new BadRequestException(
          'Deadline cannot be more than 24 hours in the future',
        );
      }

      // Convert tokenId string to number for signature generation
      const tokenIdNumber = parseInt(tokenId, 10);
      if (isNaN(tokenIdNumber)) {
        throw new BadRequestException({
          error: 'INVALID_TOKEN_ID',
          message: 'Token ID must be a valid number',
        });
      }

      // Verify token exists and get podium data
      let podiumData;
      try {
        podiumData = await this.podiumService.getPodiumData(tokenId);
      } catch (error) {
        throw new BadRequestException({
          error: 'TOKEN_NOT_FOUND',
          message: 'Token does not exist',
        });
      }

      // Verify buyer is not the current owner
      if (podiumData.ownerFid === BigInt(session.sub)) {
        throw new ForbiddenException({
          error: 'ALREADY_OWNER',
          message: 'You are already the owner of this podium',
        });
      }

      // Calculate current price
      const price = this.podiumService.calculatePrice(podiumData.claimCount);

      // Generate signature
      const signature = await this.signatureService.generateBuyPodiumSignature(
        session.sub,
        walletAddress,
        tokenIdNumber,
        deadline,
      );

      return {
        signature,
        price: price.toString(),
      };
    } catch (error) {
      if (
        error instanceof BadRequestException ||
        error instanceof ForbiddenException
      ) {
        throw error;
      }

      logger.error('Failed to generate buy podium signature:', error);
      throw new InternalServerErrorException(
        error.message || 'Failed to generate buy podium signature',
      );
    }
  }

  /**
   * Claim Fees Signature
   * Generates an EIP-712 signature authorizing a podium owner to claim accumulated fees
   */
  @Post('/podium/claim-fees-signature')
  @UseGuards(AuthorizationGuard)
  async claimFeesSignature(
    @Session() session: QuickAuthPayload,
    @Body() body: ClaimFeesSignatureDto,
  ) {
    try {
      logger.log(
        `💵 [PODIUM] Claim fees signature request for FID: ${session.sub}, TokenId: ${body.tokenId}`,
      );

      const { walletAddress, tokenId, deadline } = body;

      // Validate deadline
      const maxDeadline = Math.floor(Date.now() / 1000) + 24 * 60 * 60;
      if (deadline > maxDeadline) {
        throw new BadRequestException(
          'Deadline cannot be more than 24 hours in the future',
        );
      }

      // Convert tokenId string to number for signature generation
      const tokenIdNumber = parseInt(tokenId, 10);
      if (isNaN(tokenIdNumber)) {
        throw new BadRequestException({
          error: 'INVALID_TOKEN_ID',
          message: 'Token ID must be a valid number',
        });
      }

      // Verify token exists and get podium data
      let podiumData;
      try {
        podiumData = await this.podiumService.getPodiumData(tokenId);
      } catch (error) {
        throw new BadRequestException({
          error: 'TOKEN_NOT_FOUND',
          message: 'Token does not exist',
        });
      }

      // Verify caller is the current owner
      if (podiumData.ownerFid !== BigInt(session.sub)) {
        throw new ForbiddenException({
          error: 'NOT_OWNER',
          message: 'You are not the owner of this podium',
        });
      }

      // Get fee claim nonce and calculate fees
      const feeClaimNonce = await this.podiumService.getFeeClaimNonce(tokenId);
      const feeAmount = await this.podiumService.calculateAccumulatedFees(
        tokenId,
        feeClaimNonce,
      );

      // Generate signature
      const signature = await this.signatureService.generateClaimFeesSignature(
        tokenIdNumber,
        deadline,
      );

      return {
        signature,
        feeAmount: feeAmount.toString(),
      };
    } catch (error) {
      if (
        error instanceof BadRequestException ||
        error instanceof ForbiddenException
      ) {
        throw error;
      }

      logger.error('Failed to generate claim fees signature:', error);
      throw new InternalServerErrorException(
        error.message || 'Failed to generate claim fees signature',
      );
    }
  }

  @Post('/collectible-activity')
  @UseGuards(IndexerGuard)
  async collectibleActivity(
    @Body()
    data: {
      tokenId: number;
      eventType: string;
      price?: string;
      fromFid: number;
      toFid?: number;
      fromWallet: string;
      toWallet?: string;
      txHash: string;
      timestamp: number;
    },
  ) {
    return this.podiumService.recordActivity(data);
  }

  @Get('/collectible-activity/:tokenId')
  @UseGuards(AuthorizationGuard)
  async getCollectibleActivity(@Param('tokenId') tokenId: number) {
    return this.podiumService.getActivity(tokenId);
  }
}
