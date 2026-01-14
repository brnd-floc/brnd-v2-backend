// src/core/admin/services/admin.service.ts
import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Like } from 'typeorm';
import { Brand, Category } from '../../../models';
import {
  CreateBrandDto,
  UpdateBrandDto,
  PrepareMetadataDto,
  BlockchainBrandDto,
} from '../dto';
import NeynarService from '../../../utils/neynar';
import { IpfsService } from '../../../utils/ipfs.service';
import { BlockchainService } from '../../blockchain/services/blockchain.service';

import { UserBrandVotes } from '../../../models';

@Injectable()
export class AdminService {
  private neynarService: NeynarService;

  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    @InjectRepository(Category)
    private readonly categoryRepository: Repository<Category>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
    private readonly ipfsService: IpfsService,
    private readonly blockchainService: BlockchainService,
  ) {
    this.neynarService = new NeynarService();
    console.log('AdminService initialized');
  }

  async fixWeeklyScores(): Promise<{ message: string; updatedBrands: number }> {
    console.log('🔄 Fixing weekly scores from votes since last reset...');

    try {
      // Calculate the last Friday 18:00 UTC (when weekly reset happened)
      const lastFridayReset = this.getLastFridayReset();

      console.log(
        `📅 Calculating scores from: ${lastFridayReset.toISOString()}`,
      );

      // Use raw query to update weekly scores
      const result = await this.brandRepository.query(
        `
        UPDATE brands b SET scoreWeek = (
          SELECT COALESCE(SUM(
            CASE 
              WHEN ubv.brand1Id = b.id THEN 60
              WHEN ubv.brand2Id = b.id THEN 30  
              WHEN ubv.brand3Id = b.id THEN 10
              ELSE 0
            END
          ), 0)
          FROM user_brand_votes ubv
          WHERE ubv.date > ?
          AND (ubv.brand1Id = b.id OR ubv.brand2Id = b.id OR ubv.brand3Id = b.id)
        )
      `,
        [lastFridayReset],
      );

      console.log('✅ Weekly scores fixed successfully');

      return {
        message: 'Weekly scores fixed successfully',
        updatedBrands: result.affectedRows || 0,
      };
    } catch (error) {
      console.error('❌ Failed to fix weekly scores:', error);
      throw new Error(`Failed to fix weekly scores: ${error.message}`);
    }
  }

  /**
   * Calculate the start of the current week (Saturday 00:00:00 UTC)
   * Week runs from Saturday 00:00:00 UTC to Friday 23:59:59 UTC
   * Week ends on Friday at midnight UTC (Saturday 00:00:00 UTC)
   * This uses the same logic as your BrandService deployment time
   */
  private getLastFridayReset(): Date {
    const now = new Date();
    const deploymentTime = new Date('2025-06-20T18:00:00.000Z');

    // Find the first Saturday 00:00:00 UTC after deployment (same as BrandService logic)
    const cycleStart = new Date(deploymentTime);
    cycleStart.setUTCHours(0, 0, 0, 0); // Set to midnight UTC

    // Find next Saturday 00:00:00 UTC
    while (cycleStart.getUTCDay() !== 6) {
      // 6 = Saturday
      cycleStart.setUTCDate(cycleStart.getUTCDate() + 1); // Add 1 day
    }

    // Calculate which cycle we're in
    const msPerWeek = 7 * 24 * 60 * 60 * 1000;
    const timeSinceFirstCycle = now.getTime() - cycleStart.getTime();
    const cycleNumber = Math.floor(timeSinceFirstCycle / msPerWeek) + 1;

    // Return the start of current cycle (most recent Saturday 00:00:00 UTC)
    return new Date(cycleStart.getTime() + (cycleNumber - 1) * msPerWeek);
  }

  async recalculateScoreDayForDay(day: number): Promise<{
    message: string;
    updatedBrands: number;
    processedVotes: number;
  }> {
    console.log(`🔄 Recalculating scoreDay for day ${day}...`);

    try {
      // Reset all scoreDay values to 0
      console.log('📊 Resetting all scoreDay values to 0...');
      const resetResult = await this.brandRepository.query(
        'UPDATE brands SET scoreDay = 0',
      );
      console.log(`✅ Reset scoreDay for all brands`);

      // Get all votes for the specified day
      console.log(`📊 Fetching votes for day ${day}...`);
      const votes = await this.userBrandVotesRepository.find({
        where: { day },
        relations: ['brand1', 'brand2', 'brand3'],
      });

      console.log(`📊 Found ${votes.length} votes for day ${day}`);

      let processedVotes = 0;
      const brandScores = new Map<number, number>();

      // Process each vote and accumulate scores
      for (const vote of votes) {
        if (
          vote.brndPaidWhenCreatingPodium &&
          vote.brndPaidWhenCreatingPodium > 0
        ) {
          const baseAmount = vote.brndPaidWhenCreatingPodium;

          // 60% to brand1
          if (vote.brand1?.id) {
            const currentScore = brandScores.get(vote.brand1.id) || 0;
            brandScores.set(vote.brand1.id, currentScore + baseAmount * 0.6);
          }

          // 30% to brand2
          if (vote.brand2?.id) {
            const currentScore = brandScores.get(vote.brand2.id) || 0;
            brandScores.set(vote.brand2.id, currentScore + baseAmount * 0.3);
          }

          // 10% to brand3
          if (vote.brand3?.id) {
            const currentScore = brandScores.get(vote.brand3.id) || 0;
            brandScores.set(vote.brand3.id, currentScore + baseAmount * 0.1);
          }

          processedVotes++;
        }
      }

      console.log(
        `📊 Processed ${processedVotes} votes with brndPaidWhenCreatingPodium values`,
      );
      console.log(`📊 Updating scores for ${brandScores.size} brands...`);

      // Update scoreDay for each brand
      let updatedBrands = 0;
      for (const [brandId, score] of brandScores.entries()) {
        await this.brandRepository.update(
          { id: brandId },
          { scoreDay: Math.round(score) },
        );
        updatedBrands++;
      }

      console.log('✅ ScoreDay recalculation completed successfully');

      return {
        message: `ScoreDay recalculated successfully for day ${day}`,
        updatedBrands,
        processedVotes,
      };
    } catch (error) {
      console.error('❌ Failed to recalculate scoreDay:', error);
      throw new Error(`Failed to recalculate scoreDay: ${error.message}`);
    }
  }

  async getAllBrands(
    page: number = 1,
    limit: number = 50,
    search: string = '',
  ): Promise<[Brand[], number]> {
    const skip = (page - 1) * limit;

    return this.brandRepository.findAndCount({
      where: search ? { name: Like(`%${search}%`) } : {},
      relations: ['category'],
      order: { name: 'ASC' },
      skip,
      take: limit,
    });
  }

  async createBrand(createBrandDto: CreateBrandDto): Promise<Brand> {
    console.log('Creating brand with data:', createBrandDto);

    // Handle channelOrProfile field from frontend
    if (createBrandDto.channelOrProfile) {
      if (createBrandDto.queryType === 0) {
        // Channel type
        createBrandDto.channel = createBrandDto.channelOrProfile.startsWith('/')
          ? createBrandDto.channelOrProfile
          : `/${createBrandDto.channelOrProfile}`;
      } else {
        // Profile type
        createBrandDto.profile = createBrandDto.channelOrProfile.startsWith('@')
          ? createBrandDto.channelOrProfile
          : `@${createBrandDto.channelOrProfile}`;
      }
    }

    // Get or create category with default fallback
    const category = await this.getOrCreateCategory(
      createBrandDto.categoryId || 'General',
    );

    // Handle profile/channel logic similar to seeding service
    const { profile, channel, queryType } =
      this.processProfileAndChannel(createBrandDto);

    // Set warpcastUrl fallback
    const warpcastUrl = createBrandDto.warpcastUrl || createBrandDto.url;

    // Fetch follower count from Neynar if possible
    let followerCount = createBrandDto.followerCount || 0;
    try {
      if (queryType === 0 && channel) {
        // Channel query - remove leading slash for API
        followerCount = await this.neynarService.getChannelFollowerCount(
          channel.startsWith('/') ? channel.slice(1) : channel,
        );
      } else if (queryType === 1 && profile) {
        // Profile query - remove leading @ for API
        followerCount = await this.neynarService.getProfileFollowerCount(
          profile.startsWith('@') ? profile.slice(1) : profile,
        );
      }
    } catch (error) {
      console.warn(
        'Failed to fetch follower count from Neynar:',
        error.message,
      );
      // Continue with provided or default value
    }

    const brand = this.brandRepository.create({
      name: createBrandDto.name,
      url: createBrandDto.url,
      warpcastUrl,
      description: createBrandDto.description,
      imageUrl: createBrandDto.imageUrl || '',
      profile,
      channel,
      queryType,
      followerCount,
      category,
      // Initialize scoring fields (like seeding service)
      score: 0,
      stateScore: 0,
      scoreDay: 0,
      stateScoreDay: 0,
      scoreWeek: 0,
      stateScoreWeek: 0,
      scoreMonth: 0,
      stateScoreMonth: 0,
      ranking: '0',
      rankingWeek: 0,
      rankingMonth: 0,
      bonusPoints: 0,
      banned: 0,
      currentRanking: 0,
    });

    const savedBrand = await this.brandRepository.save(brand);
    console.log('Brand created successfully:', savedBrand);
    return savedBrand;
  }

  async updateBrand(
    id: number,
    updateBrandDto: UpdateBrandDto,
  ): Promise<Brand> {
    console.log(`Updating brand ${id} with data:`, updateBrandDto);

    const brand = await this.brandRepository.findOne({
      where: { id },
      relations: ['category'],
    });

    if (!brand) {
      throw new Error('Brand not found');
    }

    // If category is being updated, get or create it
    if (updateBrandDto.categoryId) {
      brand.category = await this.getOrCreateCategory(
        updateBrandDto.categoryId,
      );
    }

    // Update basic fields
    if (updateBrandDto.name !== undefined) brand.name = updateBrandDto.name;
    if (updateBrandDto.url !== undefined) brand.url = updateBrandDto.url;
    if (updateBrandDto.warpcastUrl !== undefined)
      brand.warpcastUrl = updateBrandDto.warpcastUrl;
    if (updateBrandDto.description !== undefined)
      brand.description = updateBrandDto.description;
    if (updateBrandDto.imageUrl !== undefined)
      brand.imageUrl = updateBrandDto.imageUrl;

    // Handle profile/channel updates
    if (
      updateBrandDto.queryType !== undefined ||
      updateBrandDto.profile !== undefined ||
      updateBrandDto.channel !== undefined
    ) {
      const updateData = {
        ...brand,
        ...updateBrandDto,
      };

      const { profile, channel, queryType } =
        this.processProfileAndChannel(updateData);
      brand.profile = profile;
      brand.channel = channel;
      brand.queryType = queryType;
    }

    // Update follower count if needed
    if (updateBrandDto.followerCount !== undefined) {
      brand.followerCount = updateBrandDto.followerCount;
    }

    const savedBrand = await this.brandRepository.save(brand);
    console.log('Brand updated successfully:', savedBrand);
    return savedBrand;
  }

  async deleteBrand(id: number): Promise<void> {
    const brand = await this.brandRepository.findOne({ where: { id } });

    if (!brand) {
      throw new Error('Brand not found');
    }

    await this.brandRepository.remove(brand);
    console.log(`Brand ${id} deleted successfully`);
  }

  async getCategories(): Promise<Category[]> {
    return this.categoryRepository.find({
      order: { name: 'ASC' },
    });
  }

  /**
   * Refresh follower count for a specific brand from Neynar
   */
  async refreshBrandFollowerCount(id: number): Promise<Brand> {
    const brand = await this.brandRepository.findOne({
      where: { id },
      relations: ['category'],
    });

    if (!brand) {
      throw new Error('Brand not found');
    }

    try {
      let followerCount = 0;

      if (brand.queryType === 0 && brand.channel) {
        // Channel query
        followerCount = await this.neynarService.getChannelFollowerCount(
          brand.channel.startsWith('/')
            ? brand.channel.slice(1)
            : brand.channel,
        );
      } else if (brand.queryType === 1 && brand.profile) {
        // Profile query
        followerCount = await this.neynarService.getProfileFollowerCount(
          brand.profile.startsWith('@')
            ? brand.profile.slice(1)
            : brand.profile,
        );
      }

      brand.followerCount = followerCount;
      return await this.brandRepository.save(brand);
    } catch (error) {
      console.error('Failed to refresh follower count from Neynar:', error);
      throw new Error(`Failed to refresh follower count: ${error.message}`);
    }
  }

  /**
   * Get or create category (like seeding service)
   */
  private async getOrCreateCategory(
    categoryIdOrName: number | string,
  ): Promise<Category> {
    // If it's a number, try to find by ID first
    if (typeof categoryIdOrName === 'number') {
      const existingCategory = await this.categoryRepository.findOne({
        where: { id: categoryIdOrName },
      });

      if (existingCategory) {
        return existingCategory;
      }

      throw new Error(`Category with ID ${categoryIdOrName} not found`);
    }

    // If it's a string, find or create by name
    let category = await this.categoryRepository.findOne({
      where: { name: categoryIdOrName },
    });

    if (!category) {
      category = this.categoryRepository.create({
        name: categoryIdOrName,
      });
      await this.categoryRepository.save(category);
      console.log(`Created new category: ${categoryIdOrName}`);
    }

    return category;
  }

  /**
   * Process profile and channel logic (similar to seeding service)
   */
  private processProfileAndChannel(data: any): {
    profile: string;
    channel: string;
    queryType: number;
  } {
    const hasProfile = data.profile && data.profile.trim() !== '';
    const hasChannel = data.channel && data.channel.trim() !== '';

    let profile = '';
    let channel = '';
    let queryType = data.queryType || 0;

    if (hasChannel) {
      // Use provided channel
      channel = data.channel.startsWith('/')
        ? data.channel
        : `/${data.channel}`;
      queryType = 0; // Channel type
    } else if (hasProfile) {
      // Generate channel from profile or use profile
      profile = data.profile.startsWith('@')
        ? data.profile
        : `@${data.profile}`;

      if (queryType === 0) {
        // If they want channel type but only provided profile, convert
        channel = profile.replace('@', '/');
      } else {
        // Profile type
        queryType = 1;
      }
    } else {
      // No channel or profile - generate fallback
      const fallbackName = data.name.toLowerCase().replace(/\s+/g, '-');

      if (queryType === 0) {
        channel = `/${fallbackName}`;
      } else {
        profile = `@${fallbackName}`;
      }
    }

    return { profile, channel, queryType };
  }

  /**
   * Validates brand data for on-chain creation
   * Checks for conflicts, duplicates, and data integrity
   */
  async validateBrandForCreation(
    prepareMetadataDto: PrepareMetadataDto,
  ): Promise<{
    valid: boolean;
    isEditing: boolean;
    message?: string;
    conflicts?: string[];
    data?: any;
  }> {
    console.log('Validating brand data for creation:', prepareMetadataDto);

    const conflicts: string[] = [];

    try {
      // 1. Validate required fields
      if (!prepareMetadataDto.name || prepareMetadataDto.name.trim() === '') {
        conflicts.push('Brand name is required');
      }

      if (
        !prepareMetadataDto.isEditing &&
        (!prepareMetadataDto.handle || prepareMetadataDto.handle.trim() === '')
      ) {
        conflicts.push('Handle is required');
      }

      if (
        !prepareMetadataDto.isEditing &&
        (!prepareMetadataDto.fid || prepareMetadataDto.fid <= 0)
      ) {
        conflicts.push('Valid FID is required');
      }

      if (
        !prepareMetadataDto.isEditing &&
        (!prepareMetadataDto.walletAddress ||
          !/^0x[a-fA-F0-9]{40}$/.test(prepareMetadataDto.walletAddress))
      ) {
        conflicts.push('Valid wallet address is required (0x format)');
      }

      if (
        !prepareMetadataDto.description ||
        prepareMetadataDto.description.trim() === ''
      ) {
        conflicts.push('Description is required');
      }

      if (
        !prepareMetadataDto.channelOrProfile ||
        prepareMetadataDto.channelOrProfile.trim() === ''
      ) {
        conflicts.push('Channel or profile is required');
      }

      // 2. Check for duplicate names
      const existingBrandByName = await this.brandRepository.findOne({
        where: { name: prepareMetadataDto.name },
      });

      if (!prepareMetadataDto.isEditing && existingBrandByName) {
        conflicts.push(
          `Brand name "${prepareMetadataDto.name}" already exists`,
        );
      }

      // 3. Check for duplicate handles
      const existingBrandByHandle = await this.brandRepository.findOne({
        where: [
          { onChainHandle: prepareMetadataDto.handle },
          { name: prepareMetadataDto.handle },
        ],
      });

      if (!prepareMetadataDto.isEditing && existingBrandByHandle) {
        conflicts.push(`Handle "${prepareMetadataDto.handle}" already exists`);
      }

      // 4. Check for duplicate URLs
      const existingBrandByUrl = await this.brandRepository.findOne({
        where: { url: prepareMetadataDto.url },
      });

      if (!prepareMetadataDto.isEditing && existingBrandByUrl) {
        conflicts.push(
          `Website URL "${prepareMetadataDto.url}" already exists`,
        );
      }

      // 5. Check for duplicate channel/profile
      if (
        !prepareMetadataDto.isEditing &&
        prepareMetadataDto.queryType === 0 &&
        prepareMetadataDto.channelOrProfile
      ) {
        // Channel validation
        const channelName = prepareMetadataDto.channelOrProfile.startsWith('/')
          ? prepareMetadataDto.channelOrProfile
          : `/${prepareMetadataDto.channelOrProfile}`;

        const existingBrandByChannel = await this.brandRepository.findOne({
          where: { channel: channelName },
        });

        if (existingBrandByChannel) {
          conflicts.push(`Channel "${channelName}" already exists`);
        }
      } else if (
        !prepareMetadataDto.isEditing &&
        prepareMetadataDto.queryType === 1 &&
        prepareMetadataDto.channelOrProfile
      ) {
        // Profile validation
        const profileName = prepareMetadataDto.channelOrProfile.startsWith('@')
          ? prepareMetadataDto.channelOrProfile
          : `@${prepareMetadataDto.channelOrProfile}`;

        const existingBrandByProfile = await this.brandRepository.findOne({
          where: { profile: profileName },
        });

        if (existingBrandByProfile) {
          conflicts.push(`Profile "${profileName}" already exists`);
        }
      }

      // 6. Validate category exists
      if (prepareMetadataDto.categoryId) {
        let category = await this.categoryRepository.findOne({
          where: { id: prepareMetadataDto.categoryId },
        });

        if (!category) {
          // If the category does not exist, create it on the fly and continue
          category = this.categoryRepository.create({
            id: prepareMetadataDto.categoryId,
            name: `${prepareMetadataDto.categoryId.toString()}`,
          });
          await this.categoryRepository.save(category);
        }
      }

      // Return validation result
      if (conflicts.length > 0) {
        return {
          valid: false,
          isEditing: prepareMetadataDto.isEditing,
          message: `Validation failed: ${conflicts.length} conflict(s) found`,
          conflicts,
        };
      }

      return {
        valid: true,
        isEditing: prepareMetadataDto.isEditing,
        message: 'Brand validation passed successfully',
        data: {
          handle: prepareMetadataDto.handle,
          fid: prepareMetadataDto.fid,
          walletAddress: prepareMetadataDto.walletAddress,
          channelOrProfile: prepareMetadataDto.channelOrProfile,
        },
      };
    } catch (error) {
      console.error('Error during brand validation:', error);
      return {
        valid: false,
        isEditing: prepareMetadataDto.isEditing,
        message: `Validation error: ${error.message}`,
        conflicts: ['Internal validation error'],
      };
    }
  }

  /**
   * Prepares brand metadata for on-chain creation by uploading to IPFS
   * Validates handle uniqueness and returns IPFS hash
   */
  async prepareBrandMetadata(prepareMetadataDto: PrepareMetadataDto): Promise<{
    metadataHash: string;
    handle: string;
    fid: number;
    walletAddress: string;
    isEditing: boolean;
  }> {
    console.log(
      'Preparing brand metadata for IPFS upload:',
      prepareMetadataDto,
    );

    // Validate required fields
    if (!prepareMetadataDto.handle || prepareMetadataDto.handle.trim() === '') {
      throw new Error('Handle is required');
    }

    if (!prepareMetadataDto.fid || prepareMetadataDto.fid <= 0) {
      throw new Error('Valid FID is required');
    }

    if (
      !prepareMetadataDto.walletAddress ||
      !/^0x[a-fA-F0-9]{40}$/.test(prepareMetadataDto.walletAddress)
    ) {
      throw new Error('Valid wallet address is required (0x format)');
    }

    // Check handle uniqueness (check both name and onChainHandle)
    const existingBrand = await this.brandRepository.findOne({
      where: [
        { name: prepareMetadataDto.handle },
        { onChainHandle: prepareMetadataDto.handle },
      ],
    });

    if (!prepareMetadataDto.isEditing && existingBrand) {
      throw new Error(
        `Handle "${prepareMetadataDto.handle}" already exists. Please choose a different handle.`,
      );
    }

    // Create metadata JSON object (only upload BrandFormData fields to IPFS)
    const metadata = {
      name: prepareMetadataDto.name,
      url: prepareMetadataDto.url,
      warpcastUrl: prepareMetadataDto.warpcastUrl,
      description: prepareMetadataDto.description,
      categoryId: prepareMetadataDto.categoryId,
      followerCount: prepareMetadataDto.followerCount,
      imageUrl: prepareMetadataDto.imageUrl,
      profile: prepareMetadataDto.profile,
      channel: prepareMetadataDto.channel,
      queryType: prepareMetadataDto.queryType,
      channelOrProfile: prepareMetadataDto.channelOrProfile,
      createdAt: new Date().toISOString(),
    };

    // Upload to IPFS
    const metadataHash = await this.ipfsService.uploadJsonToIpfs(metadata);

    console.log('✅ Brand metadata prepared successfully:', {
      metadataHash,
      handle: prepareMetadataDto.handle,
      fid: prepareMetadataDto.fid,
      walletAddress: prepareMetadataDto.walletAddress,
    });

    return {
      metadataHash,
      handle: prepareMetadataDto.handle,
      fid: prepareMetadataDto.fid,
      walletAddress: prepareMetadataDto.walletAddress,
      isEditing: prepareMetadataDto.isEditing,
    };
  }

  /**
   * Creates or updates a brand from blockchain data sent by the indexer
   * Queries the smart contract for metadata and creates/updates brand in database
   */
  async createOrUpdateBrandFromBlockchain(
    blockchainBrandDto: BlockchainBrandDto,
  ): Promise<Brand> {
    console.log(
      `📋 [INDEXER] ${blockchainBrandDto.createdOrUpdated === 'created' ? 'Creating' : 'Updating'} brand from blockchain data:`,
      blockchainBrandDto,
    );

    try {
      // Check if brand already exists by onChainId
      const existingBrand = await this.brandRepository.findOne({
        where: { onChainId: blockchainBrandDto.id },
      });

      // If brand exists and event is 'created', just return existing brand
      if (existingBrand && blockchainBrandDto.createdOrUpdated === 'created') {
        console.log(
          `⚠️  [INDEXER] Brand with onChainId ${blockchainBrandDto.id} already exists, returning existing brand`,
        );
        return existingBrand;
      }

      // Get brand data from smart contract
      const contractBrand = await this.blockchainService.getBrandFromContract(
        blockchainBrandDto.id,
      );

      if (!contractBrand) {
        throw new Error(
          `Brand with ID ${blockchainBrandDto.id} not found in smart contract`,
        );
      }

      // Use metadataHash from DTO if provided, otherwise from contract
      const metadataHash =
        blockchainBrandDto.metadataHash || contractBrand.metadataHash;

      console.log(
        `📡 [IPFS] Using metadataHash: ${metadataHash} (from ${blockchainBrandDto.metadataHash ? 'indexer' : 'contract'})`,
      );

      // Fetch metadata from IPFS
      let metadata: any = {};
      if (metadataHash && metadataHash.length > 0) {
        try {
          metadata = await this.blockchainService.fetchMetadataFromIpfs(
            metadataHash,
          );
          console.log('📡 [IPFS] Successfully fetched metadata:', metadata);
        } catch (ipfsError) {
          console.warn(
            '⚠️  [IPFS] Failed to fetch metadata, using contract data only:',
            ipfsError.message,
          );
        }
      } else {
        console.warn('⚠️  [IPFS] No metadataHash available, skipping IPFS fetch');
      }

      // Get or create category with fallback
      const category = await this.getOrCreateCategory(
        metadata.categoryId || 'General',
      );

      // Process profile/channel logic from metadata
      const { profile, channel, queryType } = this.processProfileAndChannel({
        profile: metadata.profile,
        channel: metadata.channel,
        queryType: metadata.queryType || 0,
        name: contractBrand.handle,
      });

      // Fetch follower count from Neynar if possible
      let followerCount = metadata.followerCount || 0;
      try {
        if (queryType === 0 && channel) {
          followerCount = await this.neynarService.getChannelFollowerCount(
            channel.startsWith('/') ? channel.slice(1) : channel,
          );
        } else if (queryType === 1 && profile) {
          followerCount = await this.neynarService.getProfileFollowerCount(
            profile.startsWith('@') ? profile.slice(1) : profile,
          );
        }
      } catch (error) {
        console.warn(
          'Failed to fetch follower count from Neynar:',
          error.message,
        );
      }

      // If brand exists and event is 'updated', update existing brand
      if (existingBrand && blockchainBrandDto.createdOrUpdated === 'updated') {
        console.log(
          `🔄 [INDEXER] Updating existing brand with onChainId ${blockchainBrandDto.id}`,
        );

        // Update on-chain data (source of truth)
        existingBrand.onChainHandle = contractBrand.handle;
        existingBrand.onChainFid = contractBrand.fid;
        existingBrand.onChainWalletAddress = contractBrand.walletAddress;
        existingBrand.metadataHash = metadataHash;

        // Update metadata from IPFS (can be updated)
        existingBrand.name = metadata.name || contractBrand.handle;
        existingBrand.url = metadata.url || '';
        existingBrand.warpcastUrl = metadata.warpcastUrl || metadata.url || '';
        existingBrand.description = metadata.description || '';
        existingBrand.imageUrl = metadata.imageUrl || '';
        existingBrand.profile = metadata.profile || profile;
        existingBrand.channel = metadata.channel || channel;
        existingBrand.queryType = metadata.queryType ?? queryType;
        existingBrand.followerCount = metadata.followerCount || followerCount;
        existingBrand.category = category;

        // Note: We preserve scoring fields (score, stateScore, etc.) on update
        // These are managed separately and shouldn't be reset

        const updatedBrand = await this.brandRepository.save(existingBrand);

        console.log(
          '✅ [INDEXER] Brand updated successfully from blockchain:',
          {
            id: updatedBrand.id,
            onChainId: updatedBrand.onChainId,
            name: updatedBrand.name,
            handle: updatedBrand.onChainHandle,
            fid: updatedBrand.onChainFid,
            metadataHash: updatedBrand.metadataHash,
          },
        );

        return updatedBrand;
      }

      // Create new brand entity
      const brand = this.brandRepository.create({
        // On-chain data (source of truth)
        onChainId: blockchainBrandDto.id,
        onChainHandle: contractBrand.handle,
        onChainFid: contractBrand.fid,
        onChainWalletAddress: contractBrand.walletAddress,
        onChainCreatedAt: new Date(contractBrand.createdAt * 1000),
        metadataHash: metadataHash,

        // Metadata from IPFS (can be updated)
        name: metadata.name || contractBrand.handle,
        url: metadata.url || '',
        warpcastUrl: metadata.warpcastUrl || metadata.url || '',
        description: metadata.description || '',
        imageUrl: metadata.imageUrl || '',
        profile: metadata.profile || profile,
        channel: metadata.channel || channel,
        queryType: metadata.queryType ?? queryType,
        followerCount: metadata.followerCount || followerCount,
        category,

        // Initialize scoring fields
        score: 0,
        stateScore: 0,
        scoreWeek: 0,
        stateScoreWeek: 0,
        scoreMonth: 0,
        stateScoreMonth: 0,
        ranking: '0',
        rankingWeek: 0,
        rankingMonth: 0,
        bonusPoints: 0,
        banned: 0,
        currentRanking: 0,

        // Blockchain info
        totalBrndAwarded: contractBrand.totalBrndAwarded.toString(),
        availableBrnd: contractBrand.availableBrnd.toString(),
      });

      const savedBrand = await this.brandRepository.save(brand);

      console.log('✅ [INDEXER] Brand created successfully from blockchain:', {
        id: savedBrand.id,
        onChainId: savedBrand.onChainId,
        name: savedBrand.name,
        handle: savedBrand.onChainHandle,
        fid: savedBrand.onChainFid,
        metadataHash: savedBrand.metadataHash,
      });

      return savedBrand;
    } catch (error) {
      console.error(
        `❌ [INDEXER] Failed to ${blockchainBrandDto.createdOrUpdated === 'created' ? 'create' : 'update'} brand from blockchain:`,
        error,
      );
      throw new Error(
        `Failed to ${blockchainBrandDto.createdOrUpdated === 'created' ? 'create' : 'update'} brand from blockchain: ${error.message}`,
      );
    }
  }
}
