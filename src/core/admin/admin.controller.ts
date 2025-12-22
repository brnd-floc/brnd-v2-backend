// src/core/admin/admin.controller.ts
import {
  Body,
  Controller,
  Get,
  Post,
  Put,
  Delete,
  Param,
  Query,
  Res,
  UseGuards,
} from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { Response } from 'express';
import { AdminService } from './services/admin.service';
import { ContractUploadService } from '../blockchain/services/contract-upload.service';
import { AirdropService } from '../airdrop/services/airdrop.service';
import { PodiumService } from '../embeds/services/podium.service';
import { FarcasterNotificationService } from '../notification/services/farcaster-notification.service';
import {
  CreateBrandDto,
  UpdateBrandDto,
  PrepareMetadataDto,
  BlockchainBrandDto,
} from './dto';
import {
  AdminGuard,
  AuthorizationGuard,
  QuickAuthPayload,
} from '../../security/guards';
import { Session } from '../../security/decorators';
import { HttpStatus, hasError, hasResponse } from '../../utils';
import { logger } from '../../main';
import { createWalletClient, http, parseAbi } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { base } from 'viem/chains';
import { getConfig } from '../../security/config';

const adminFids = [5431, 16098];

@ApiTags('admin-service')
@Controller('admin-service')
export class AdminController {
  constructor(
    private readonly adminService: AdminService,
    private readonly contractUploadService: ContractUploadService,
    private readonly airdropService: AirdropService,
    private readonly podiumService: PodiumService,
    private readonly farcasterNotificationService: FarcasterNotificationService,
  ) {
    console.log('AdminController initialized');
  }

  /**
   * Get all brands for admin management
   */
  @Get('brands')
  @UseGuards(AuthorizationGuard)
  async getAllBrands(
    @Session() user: QuickAuthPayload,
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 50,
    @Query('search') search: string = '',
    @Res() res: Response,
  ) {
    console.log(
      `getAllBrands called - user: ${user.sub}, page: ${page}, limit: ${limit}, search: "${search}"`,
    );

    // Check admin permissions
    if (!adminFids.includes(user.sub)) {
      console.log(`Access denied for user ${user.sub} - not in admin list`);
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'getAllBrands',
        'Admin access required',
      );
    }

    try {
      console.log('Fetching brands from service...');
      const [brands, count] = await this.adminService.getAllBrands(
        page,
        limit,
        search,
      );
      console.log(
        `Found ${count} total brands, returning ${brands.length} results`,
      );

      return hasResponse(res, {
        brands,
        pagination: {
          page,
          limit,
          total: count,
          totalPages: Math.ceil(count / limit),
        },
      });
    } catch (error) {
      console.error('Error in getAllBrands:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getAllBrands',
        error.message,
      );
    }
  }

  /**
   * Create a new brand
   */
  @Post('brands')
  async createBrand(
    @Session() user: QuickAuthPayload,
    @Body() createBrandDto: CreateBrandDto,
    @Res() res: Response,
  ) {
    console.log(`createBrand called - user: ${user.sub}`, createBrandDto);

    if (!adminFids.includes(user.sub)) {
      console.log(`Access denied for user ${user.sub} - not in admin list`);
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'createBrand',
        'Admin access required',
      );
    }

    try {
      console.log('Creating new brand...');
      const brand = await this.adminService.createBrand(createBrandDto);
      console.log('Brand created successfully:', {
        id: brand.id,
        name: brand.name,
        queryType: brand.queryType,
        profile: brand.profile,
        channel: brand.channel,
        followerCount: brand.followerCount,
      });

      return hasResponse(res, {
        brand,
        message: 'Brand created successfully',
      });
    } catch (error) {
      console.error('Error in createBrand:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'createBrand',
        error.message,
      );
    }
  }

  /**
   * Update an existing brand
   */
  @Put('brands/:id')
  async updateBrand(
    @Session() user: QuickAuthPayload,
    @Param('id') id: number,
    @Body() updateBrandDto: UpdateBrandDto,
    @Res() res: Response,
  ) {
    console.log(
      `updateBrand called - user: ${user.sub}, id: ${id}`,
      updateBrandDto,
    );

    if (!adminFids.includes(user.sub)) {
      console.log(`Access denied for user ${user.sub} - not in admin list`);
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'updateBrand',
        'Admin access required',
      );
    }

    try {
      console.log(`Updating brand ${id}...`);

      // Handle channelOrProfile field from frontend
      if (updateBrandDto.channelOrProfile) {
        if (updateBrandDto.queryType === 0) {
          // Channel type
          updateBrandDto.channel = updateBrandDto.channelOrProfile.startsWith(
            '/',
          )
            ? updateBrandDto.channelOrProfile
            : `/${updateBrandDto.channelOrProfile}`;
        } else {
          // Profile type
          updateBrandDto.profile = updateBrandDto.channelOrProfile.startsWith(
            '@',
          )
            ? updateBrandDto.channelOrProfile
            : `@${updateBrandDto.channelOrProfile}`;
        }
      }

      const brand = await this.adminService.updateBrand(id, updateBrandDto);
      console.log('Brand updated successfully:', {
        id: brand.id,
        name: brand.name,
        queryType: brand.queryType,
        profile: brand.profile,
        channel: brand.channel,
        followerCount: brand.followerCount,
      });

      return hasResponse(res, {
        brand,
        message: 'Brand updated successfully',
      });
    } catch (error) {
      console.error('Error in updateBrand:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'updateBrand',
        error.message,
      );
    }
  }

  /**
   * Delete a brand
   */
  @Delete('brands/:id')
  async deleteBrand(
    @Session() user: QuickAuthPayload,
    @Param('id') id: number,
    @Res() res: Response,
  ) {
    console.log(`deleteBrand called - user: ${user.sub}, id: ${id}`);

    if (!adminFids.includes(user.sub)) {
      console.log(`Access denied for user ${user.sub} - not in admin list`);
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'deleteBrand',
        'Admin access required',
      );
    }

    try {
      console.log(`Deleting brand ${id}...`);
      await this.adminService.deleteBrand(id);
      console.log('Brand deleted successfully');
      return hasResponse(res, {
        message: 'Brand deleted successfully',
      });
    } catch (error) {
      console.error('Error in deleteBrand:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'deleteBrand',
        error.message,
      );
    }
  }

  /**
   * Get all categories for brand creation/editing
   */
  @Get('categories')
  async getCategories(@Session() user: QuickAuthPayload, @Res() res: Response) {
    console.log(`getCategories called - user: ${user.sub}`);

    if (!adminFids.includes(user.sub)) {
      console.log(`Access denied for user ${user.sub} - not in admin list`);
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'getCategories',
        'Admin access required',
      );
    }

    try {
      console.log('Fetching categories...');
      const categories = await this.adminService.getCategories();
      console.log(`Found ${categories.length} categories`);
      return hasResponse(res, { categories });
    } catch (error) {
      console.error('Error in getCategories:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getCategories',
        error.message,
      );
    }
  }

  /**
   * Refresh follower count for a specific brand from Neynar
   */
  @Post('brands/:id/refresh-followers')
  async refreshBrandFollowerCount(
    @Session() user: QuickAuthPayload,
    @Param('id') id: number,
    @Res() res: Response,
  ) {
    console.log(
      `refreshBrandFollowerCount called - user: ${user.sub}, id: ${id}`,
    );

    if (!adminFids.includes(user.sub)) {
      console.log(`Access denied for user ${user.sub} - not in admin list`);
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'refreshBrandFollowerCount',
        'Admin access required',
      );
    }

    try {
      console.log(`Refreshing follower count for brand ${id}...`);
      const brand = await this.adminService.refreshBrandFollowerCount(id);
      console.log('Follower count refreshed successfully:', {
        id: brand.id,
        name: brand.name,
        followerCount: brand.followerCount,
      });

      return hasResponse(res, {
        brand,
        message: 'Follower count refreshed successfully',
      });
    } catch (error) {
      console.error('Error in refreshBrandFollowerCount:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'refreshBrandFollowerCount',
        error.message,
      );
    }
  }

  @Get('fix-weekly-scores')
  @UseGuards(AuthorizationGuard)
  async fixWeeklyScores(
    @Session() user: QuickAuthPayload,
    @Res() res: Response,
  ) {
    // Check admin authorization (your existing pattern)
    const adminFids = [39278, 16098];
    if (!adminFids.includes(user.sub)) {
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'fixWeeklyScores',
        'Admin access required',
      );
    }

    try {
      const result = await this.adminService.fixWeeklyScores();
      return hasResponse(res, result);
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'fixWeeklyScores',
        error.message,
      );
    }
  }

  // @Get('recalculate-score-day/20441')
  // async recalculateScoreDayFor20441(@Res() res: Response) {
  //   console.log(`recalculateScoreDayFor20441 called`);

  //   try {
  //     const result = await this.adminService.recalculateScoreDayForDay(20441);
  //     console.log('ScoreDay recalculation result:', result);

  //     return hasResponse(res, {
  //       success: true,
  //       message: result.message,
  //       data: {
  //         day: 20441,
  //         updatedBrands: result.updatedBrands,
  //         processedVotes: result.processedVotes,
  //       },
  //     });
  //   } catch (error) {
  //     console.error('Error in recalculateScoreDayFor20441:', error);
  //     return hasError(
  //       res,
  //       HttpStatus.INTERNAL_SERVER_ERROR,
  //       'recalculateScoreDayFor20441',
  //       error.message,
  //     );
  //   }
  // }

  /**
   * Bulk operations endpoint for advanced admin tasks
   */
  @Post('brands/bulk-refresh-followers')
  async bulkRefreshFollowerCounts(
    @Session() user: QuickAuthPayload,
    @Body() { brandIds }: { brandIds: number[] },
    @Res() res: Response,
  ) {
    console.log(
      `bulkRefreshFollowerCounts called - user: ${user.sub}, brandIds: ${brandIds?.join(', ')}`,
    );

    if (!adminFids.includes(user.sub)) {
      console.log(`Access denied for user ${user.sub} - not in admin list`);
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'bulkRefreshFollowerCounts',
        'Admin access required',
      );
    }

    if (!brandIds || !Array.isArray(brandIds) || brandIds.length === 0) {
      return hasError(
        res,
        HttpStatus.BAD_REQUEST,
        'bulkRefreshFollowerCounts',
        'brandIds array is required',
      );
    }

    try {
      console.log(
        `Bulk refreshing follower counts for ${brandIds.length} brands...`,
      );
      const results = [];
      const errors = [];

      for (const brandId of brandIds) {
        try {
          const brand =
            await this.adminService.refreshBrandFollowerCount(brandId);
          results.push({
            id: brand.id,
            name: brand.name,
            followerCount: brand.followerCount,
            success: true,
          });
        } catch (error) {
          console.error(`Failed to refresh brand ${brandId}:`, error.message);
          errors.push({
            id: brandId,
            error: error.message,
            success: false,
          });
        }
      }

      console.log(
        `Bulk refresh completed: ${results.length} success, ${errors.length} errors`,
      );

      return hasResponse(res, {
        message: `Bulk refresh completed: ${results.length} success, ${errors.length} errors`,
        results,
        errors,
        summary: {
          total: brandIds.length,
          successful: results.length,
          failed: errors.length,
        },
      });
    } catch (error) {
      console.error('Error in bulkRefreshFollowerCounts:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'bulkRefreshFollowerCounts',
        error.message,
      );
    }
  }


  /**
   * Prepares brand metadata for on-chain creation
   * Validates data first, then uploads metadata to IPFS and returns hash for smart contract
   */
  @Post('brands/prepare-metadata')
  @UseGuards(AuthorizationGuard)
  async prepareBrandMetadata(
    @Session() user: QuickAuthPayload,
    @Body() prepareMetadataDto: PrepareMetadataDto,
    @Res() res: Response,
  ) {
    console.log(
      `prepareBrandMetadata called - user: ${user.sub}`,
      prepareMetadataDto,
    );

    if (!adminFids.includes(user.sub)) {
      console.log(`Access denied for user ${user.sub} - not in admin list`);
      return hasError(
        res,
        HttpStatus.FORBIDDEN,
        'prepareBrandMetadata',
        'Admin access required',
      );
    }

    try {
      // Step 1: Validate brand data first
      console.log('Validating brand data...');
      const validation = await this.adminService.validateBrandForCreation(prepareMetadataDto);
      
      if (!validation.valid) {
        console.log('Brand validation failed:', validation);
        return hasResponse(res, {
          success: false,
          valid: false,
          message: validation.message,
          conflicts: validation.conflicts || [],
        });
      }

      console.log('Brand validation passed, proceeding to metadata preparation...');

      // Step 2: Prepare metadata and upload to IPFS
      const result = await this.adminService.prepareBrandMetadata(prepareMetadataDto);
      console.log('Brand metadata prepared successfully:', result);

      return hasResponse(res, {
        success: true,
        valid: true,
        metadataHash: result.metadataHash,
        handle: result.handle,
        fid: result.fid,
        walletAddress: result.walletAddress,
        message: 'Brand validation passed and metadata uploaded to IPFS. Use the metadataHash to create the brand on-chain.',
      });
    } catch (error) {
      console.error('Error in prepareBrandMetadata:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'prepareBrandMetadata',
        error.message,
      );
    }
  }

  /**
   * Check contract sync status - compares database brands vs contract brands
   * TESTING MODE: Auth disabled
   */
  @Get('brands/contract-status')
  async getContractStatus(@Res() res: Response) {
    logger.log(`getContractStatus called - testing mode (no auth)`);

    // TESTING: Admin check disabled
    // if (!adminFids.includes(user.sub)) {
    //   logger.log(`Access denied for user ${user.sub} - not in admin list`);
    //   return hasError(
    //     res,
    //     HttpStatus.FORBIDDEN,
    //     'getContractStatus',
    //     'Admin access required',
    //   );
    // }

    try {
      logger.log('Checking contract sync status...');
      const status = await this.contractUploadService.checkContractStatus();

      logger.log('Contract status:', {
        database: status.database.totalBrands,
        contract: status.contract.totalBrands,
        needsUpload: status.sync.needsUpload,
        difference: status.sync.difference,
      });

      return hasResponse(res, {
        ...status,
        message: status.sync.needsUpload
          ? `${status.sync.difference} brands need to be uploaded to contract`
          : 'Database and contract are in sync',
      });
    } catch (error) {
      logger.error('Error checking contract status:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getContractStatus',
        `Failed to check contract status: ${error.message}`,
      );
    }
  }

  /**
   * Upload all brands from database to smart contract
   */
  @Get('brands/upload-all-brands-to-contract')
  // UPLOAD BRANDS TO SMART CONTRACT
  async uploadBrandsToContract(@Res() res: Response) {
    logger.log(`uploadBrandsToContract called - testing mode (no auth)`);

    try {
      logger.log('Starting brand upload to contract...');

      // 1. Get all brands from database
      const brands = await this.contractUploadService.getAllBrandsForContract();
      logger.log(`Found ${brands.length} brands in database`);

      if (brands.length === 0) {
        return hasResponse(res, {
          success: true,
          message: 'No brands found in database to upload',
          summary: {
            totalBrands: 0,
            batchesProcessed: 0,
            successfulBrands: 0,
            failedBrands: 0,
            gasUsed: 0,
            transactionHashes: [],
          },
        });
      }

      // 2. Validate data
      const validation =
        this.contractUploadService.validateBrandsForContract(brands);
      if (!validation.valid) {
        logger.error('Brand validation failed:', validation.issues);
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'uploadBrandsToContract',
          `Validation failed: ${validation.issues.join(', ')}`,
        );
      }

      logger.log('✅ Brand validation passed');

      // Organize the brands array by their id in ascending order before proceeding
      brands.sort((a, b) => a.id - b.id);
      console.log('*************************************************');
      console.log('*************************************************');
      console.log('*************************************************');
      console.log('*************************************************');
      console.log('*************************************************');
      console.log('IN HERE, THE BRANDS SORTED ARE:', brands);
      console.log('*************************************************');
      console.log('*************************************************');
      console.log('*************************************************');
      console.log('*************************************************');
      console.log(
        '***********************we will return now**************************',
      );
      console.log('*************************************************');

      // 3. Upload to contract in batches

      const result =
        await this.contractUploadService.uploadBrandsToContract(brands);

      const summary = {
        totalBrands: brands.length,
        batchesProcessed: result.batchesProcessed,
        successfulBrands: result.successfulBrands,
        failedBrands: result.failedBrands,
        gasUsed: result.totalGasUsed,
        transactionHashes: result.txHashes,
      };

      if (result.errors.length > 0) {
        logger.error('Some batches failed during upload:', result.errors);
      }

      const success = result.successfulBrands > 0;
      const message = success
        ? `Upload completed: ${result.successfulBrands}/${brands.length} brands uploaded successfully`
        : 'Upload failed: No brands were uploaded successfully';

      return hasResponse(res, {
        success,
        message,
        summary,
        errors: result.errors.length > 0 ? result.errors : undefined,
      });
    } catch (error) {
      logger.error('Critical error during brand upload:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'uploadBrandsToContract',
        `Upload failed: ${error.message}`,
      );
    }
  }

  /**
   * Preview what would happen during brand upload (dry run)
   */
  @Get('brands/upload-preview')
  async previewBrandUpload(@Res() res: Response) {
    logger.log(`previewBrandUpload called - testing mode (no auth)`);

    // TESTING: Admin check disabled
    // if (!adminFids.includes(user.sub)) {
    //   logger.log(`Access denied for user ${user.sub} - not in admin list`);
    //   return hasError(
    //     res,
    //     HttpStatus.FORBIDDEN,
    //     'previewBrandUpload',
    //     'Admin access required',
    //   );
    // }

    try {
      logger.log('Generating brand upload preview...');

      // Get all brands that would be uploaded
      const brands = await this.contractUploadService.getAllBrandsForContract();
      logger.log(`Found ${brands.length} brands for preview`);

      // Validate the data
      const validation =
        this.contractUploadService.validateBrandsForContract(brands);

      // Calculate batches
      const batchSize = 20; // Same as in the service
      const totalBatches = Math.ceil(brands.length / batchSize);

      // Sample of brands that would be uploaded
      const sampleBrands = brands.slice(0, 5).map((brand) => ({
        handle: brand.handle,
        fid: brand.fid,
        walletAddress: brand.walletAddress,
        hasMetadata: !!brand.metadataHash,
      }));

      return hasResponse(res, {
        preview: {
          totalBrands: brands.length,
          totalBatches,
          batchSize,
          sampleBrands,
          validation: {
            valid: validation.valid,
            issues: validation.issues,
          },
          estimatedGasCost: 'Gas estimation requires actual contract call',
          warning: 'This is a preview only. No brands will be uploaded.',
        },
        message: validation.valid
          ? `Ready to upload ${brands.length} brands in ${totalBatches} batches`
          : `Validation failed: ${validation.issues.length} issues found`,
      });
    } catch (error) {
      logger.error('Error generating upload preview:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'previewBrandUpload',
        `Preview failed: ${error.message}`,
      );
    }
  }

  /**
   * Upload limited number of brands for testing (e.g., 20 brands)
   */
  @Post('brands/upload-to-contract-testing')
  async uploadLimitedBrandsToContract(
    @Body() body: { limit?: number },
    @Res() res: Response,
  ) {
    const limit = body.limit || 20; // Default to 20 for testing
    logger.log(
      `uploadLimitedBrandsToContract called - limit: ${limit} (no auth)`,
    );

    try {
      logger.log(
        `Starting limited brand upload to contract (${limit} brands)...`,
      );

      // Get limited brands from database (non-uploaded only)
      const brands =
        await this.contractUploadService.getAllBrandsForContract(limit);
      logger.log(
        `Found ${brands.length} non-uploaded brands (requesting ${limit})`,
      );

      if (brands.length === 0) {
        return hasResponse(res, {
          success: true,
          message: 'No non-uploaded brands found for limited upload',
          summary: {
            totalBrands: 0,
            batchesProcessed: 0,
            successfulBrands: 0,
            failedBrands: 0,
            gasUsed: 0,
            transactionHashes: [],
          },
        });
      }

      // Validate brands
      const validation =
        this.contractUploadService.validateBrandsForContract(brands);

      if (!validation.valid) {
        logger.error('Brand validation failed:', validation.issues);
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'uploadLimitedBrandsToContract',
          `Validation failed: ${validation.issues.join(', ')}`,
        );
      }

      logger.log('✅ Brand validation passed');

      // Upload to contract (don't reset flags - incremental upload)
      const result = await this.contractUploadService.uploadBrandsToContract(
        brands,
        false,
      );

      const summary = {
        totalBrands: brands.length,
        batchesProcessed: result.batchesProcessed,
        successfulBrands: result.successfulBrands,
        failedBrands: result.failedBrands,
        gasUsed: result.totalGasUsed,
        transactionHashes: result.txHashes,
      };

      if (result.errors.length > 0) {
        logger.error(
          'Some batches failed during limited upload:',
          result.errors,
        );
      }

      const success = result.successfulBrands > 0;
      const message = success
        ? `Limited upload completed: ${result.successfulBrands}/${brands.length} brands uploaded successfully`
        : 'Limited upload failed: No brands were uploaded successfully';

      return hasResponse(res, {
        success,
        message,
        summary,
        errors: result.errors.length > 0 ? result.errors : undefined,
      });
    } catch (error) {
      logger.error('Critical error during limited brand upload:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'uploadLimitedBrandsToContract',
        `Limited upload failed: ${error.message}`,
      );
    }
  }

  /**
   * View upload status of all brands
   */
  @Get('brands/upload-status')
  async getBrandUploadStatus(@Res() res: Response) {
    logger.log(`getBrandUploadStatus called (no auth)`);

    try {
      const totalBrands =
        await this.contractUploadService.getDatabaseBrandCount();
      const uploadedCount =
        await this.contractUploadService.getUploadedBrandCount();
      const remainingCount = totalBrands - uploadedCount;

      return hasResponse(res, {
        totalBrands,
        uploadedBrands: uploadedCount,
        remainingBrands: remainingCount,
        uploadProgress:
          totalBrands > 0 ? Math.round((uploadedCount / totalBrands) * 100) : 0,
        message:
          remainingCount > 0
            ? `${remainingCount} brands remaining to upload`
            : 'All brands are uploaded to contract',
      });
    } catch (error) {
      logger.error('Error getting brand upload status:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getBrandUploadStatus',
        `Failed to get upload status: ${error.message}`,
      );
    }
  }

  /**
   * Reset upload flags (for new contract deployment)
   */
  @Post('brands/reset-upload-flags')
  async resetUploadFlags(@Res() res: Response) {
    logger.log(`resetUploadFlags called (no auth)`);

    try {
      await this.contractUploadService.resetUploadFlags();
      const totalBrands =
        await this.contractUploadService.getDatabaseBrandCount();

      return hasResponse(res, {
        success: true,
        message: `Reset upload flags for ${totalBrands} brands`,
        totalBrands,
        note: 'All brands are now marked as non-uploaded. Ready for fresh contract deployment.',
      });
    } catch (error) {
      logger.error('Error resetting upload flags:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'resetUploadFlags',
        `Failed to reset flags: ${error.message}`,
      );
    }
  }

  @Get('podiums/generate-sample-image')
  @UseGuards(AdminGuard)
  async generateSampleImage(@Res() res: Response) {
    logger.log(`generateSampleImage called - testing mode (no auth)`);

    try {
      const result = await this.podiumService.generateSamplePodiumImage();

      logger.log(
        `Podium image generated successfully: ${result.cloudinaryUrl}`,
      );

      // Return the image as PNG
      res.setHeader('Content-Type', 'image/png');
      res.setHeader('Cache-Control', 'no-cache');
      return res.send(result.buffer);
    } catch (error) {
      logger.error('Error generating sample podium image:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'generateSampleImage',
        error.message,
      );
    }
  }
}
