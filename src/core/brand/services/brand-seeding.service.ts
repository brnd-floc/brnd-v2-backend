// src/core/brand/services/brand-seeding.service.ts
import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import * as fs from 'fs';
import * as path from 'path';

// Models
import { Brand, Category } from '../../../models';

// Utils
import { logger } from '../../../main';

interface BrandSeedData {
  name: string;
  url?: string;
  warpcastUrl: string;
  description: string;
  followerCount: number;
  iconLogoUrl: string;
  profile?: string;
  channel?: string;
  ranking?: string;
  score?: number;
  stateScore?: number;
  scoreDay?: number;
  stateScoreDay?: number;
  scoreWeek?: number;
  stateScoreWeek?: number;
  rankingWeek?: number;
  scoreMonth?: number;
  stateScoreMonth?: number;
  rankingMonth?: number;
  bonusPoints?: number;
  banned?: number;
  queryType?: number;
  category: string;
}

/**
 * Brand seeding service for local development.
 * Handles reading brands-seed.json and populating the database.
 * Now supports brands with or without channels.
 */
@Injectable()
export class BrandSeederService {
  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,

    @InjectRepository(Category)
    private readonly categoryRepository: Repository<Category>,
  ) {}

  /**
   * Seeds brands from brands-seed.json file.
   *
   * @param overwrite - Whether to update existing brands
   * @returns Seeding results with statistics
   */
  async seedBrands(overwrite: boolean = false): Promise<{
    seeded: number;
    skipped: number;
    errors: string[];
    categories: string[];
    warnings: string[];
  }> {
    logger.log('🌱 Starting brand seeding process...');

    const result = {
      seeded: 0,
      skipped: 0,
      errors: [] as string[],
      categories: [] as string[],
      warnings: [] as string[],
    };

    try {
      // Read seed file
      const seedData = await this.readSeedFile();
      logger.log(`📊 Found ${seedData.length} brands in seed file`);

      // Process each brand
      for (const brandData of seedData) {
        try {
          const wasProcessed = await this.processBrand(
            brandData,
            overwrite,
            result.warnings,
          );

          if (wasProcessed) {
            result.seeded++;
            logger.log(`✅ Processed: ${brandData.name}`);
          } else {
            result.skipped++;
            logger.log(`⏭️  Skipped: ${brandData.name} (already exists)`);
          }

          // Track unique categories
          if (!result.categories.includes(brandData.category)) {
            result.categories.push(brandData.category);
          }
        } catch (error) {
          result.errors.push(`${brandData.name}: ${error.message}`);
          logger.error(`❌ Error with ${brandData.name}:`, error.message);
        }
      }

      logger.log(`🎉 Seeding completed!
        ✅ Processed: ${result.seeded}
        ⏭️  Skipped: ${result.skipped}  
        ❌ Errors: ${result.errors.length}
        ⚠️  Warnings: ${result.warnings.length}
        📁 Categories: ${result.categories.length}`);

      return result;
    } catch (error) {
      logger.error('💥 Seeding failed:', error.message);
      throw error;
    }
  }

  /**
   * Reads and validates brands-seed.json
   */
  private async readSeedFile(): Promise<BrandSeedData[]> {
    const seedFilePath = path.join(process.cwd(), 'brands-seed.json');

    if (!fs.existsSync(seedFilePath)) {
      throw new Error(`Seed file not found at: ${seedFilePath}`);
    }

    const fileContent = fs.readFileSync(seedFilePath, 'utf8');
    const seedData: BrandSeedData[] = JSON.parse(fileContent);

    if (!Array.isArray(seedData)) {
      throw new Error('Seed file must contain an array of brands');
    }

    // Validate each brand
    for (let i = 0; i < seedData.length; i++) {
      try {
        this.validateBrandData(seedData[i]);
      } catch (error) {
        throw new Error(`Brand at index ${i}: ${error.message}`);
      }
    }

    return seedData;
  }

  /**
   * Validates brand data structure with flexible field requirements
   */
  private validateBrandData(brand: any): void {
    // Required fields
    if (!brand.name || typeof brand.name !== 'string') {
      throw new Error('Name is required and must be a string');
    }

    if (!brand.category || typeof brand.category !== 'string') {
      throw new Error('Category is required and must be a string');
    }

    if (!brand.iconLogoUrl || typeof brand.iconLogoUrl !== 'string') {
      throw new Error('iconLogoUrl is required and must be a string');
    }

    if (!brand.warpcastUrl || typeof brand.warpcastUrl !== 'string') {
      throw new Error('warpcastUrl is required and must be a string');
    }

    // Profile is optional - can be empty string or missing
    if (brand.profile !== undefined && typeof brand.profile !== 'string') {
      throw new Error('Profile must be a string if provided');
    }

    // Channel is optional - can be empty string or missing
    if (brand.channel !== undefined && typeof brand.channel !== 'string') {
      throw new Error('Channel must be a string if provided');
    }

    // Optional numeric fields validation
    if (
      brand.followerCount !== undefined &&
      typeof brand.followerCount !== 'number'
    ) {
      throw new Error('followerCount must be a number if provided');
    }

    // Profile format validation (only if profile exists and is not empty)
    if (
      brand.profile &&
      brand.profile.trim() !== '' &&
      !brand.profile.startsWith('@')
    ) {
      throw new Error(
        `Profile must start with @ if provided (got: ${brand.profile})`,
      );
    }

    // Channel format validation (only if channel exists and is not empty)
    if (
      brand.channel &&
      brand.channel.trim() !== '' &&
      !brand.channel.startsWith('/')
    ) {
      throw new Error(
        `Channel must start with / if provided (got: ${brand.channel})`,
      );
    }
  }

  /**
   * Processes a single brand - creates or updates
   * Handles brands with profiles, channels, both, or neither
   */
  private async processBrand(
    brandData: BrandSeedData,
    overwrite: boolean,
    warnings: string[],
  ): Promise<boolean> {
    console.log(`🔄 Processing brand: ${brandData.name}`);

    // Check if brand exists
    const existingBrand = await this.brandRepository.findOne({
      where: { name: brandData.name },
      relations: ['category'],
    });

    if (existingBrand && !overwrite) {
      return false; // Skip existing
    }

    // Get or create category
    const category = await this.getOrCreateCategory(brandData.category);

    // Create or update brand
    const brand = existingBrand || this.brandRepository.create();

    // Map basic fields
    brand.name = brandData.name;
    brand.description = brandData.description;
    brand.category = category;
    brand.followerCount = brandData.followerCount || 0;
    brand.imageUrl = brandData.iconLogoUrl;

    // Handle URL - use provided URL or fallback to warpcastUrl
    if (brandData.url && brandData.url.trim() !== '') {
      brand.url = brandData.url;
    } else {
      brand.url = brandData.warpcastUrl;
      warnings.push(
        `${brandData.name}: No URL provided, using warpcastUrl as fallback`,
      );
    }

    // Handle profile - can be empty string or missing
    const hasProfile = brandData.profile && brandData.profile.trim() !== '';
    brand.profile = hasProfile ? brandData.profile : '';

    // Handle channel - can be empty string or missing
    const hasChannel = brandData.channel && brandData.channel.trim() !== '';

    if (hasChannel) {
      // Use provided channel
      brand.channel = brandData.channel;
      brand.queryType = 0; // Channel type
    } else if (hasProfile) {
      // Generate channel from profile
      brand.channel = brandData.profile.replace('@', '/');
      brand.queryType = 1; // Profile type (but using as channel)
      warnings.push(
        `${brandData.name}: No channel provided, generated from profile: ${brand.channel}`,
      );
    } else {
      // No channel or profile - use name as fallback
      const fallbackChannel = `/${brandData.name.toLowerCase().replace(/\s+/g, '-')}`;
      brand.channel = fallbackChannel;
      brand.queryType = 1; // Profile type
      warnings.push(
        `${brandData.name}: No channel or profile, generated fallback: ${fallbackChannel}`,
      );
    }

    // Set warpcastUrl from seed data
    brand.warpcastUrl = brandData.warpcastUrl;

    // Set other required fields with defaults from seed data or reasonable defaults
    brand.ranking = (brandData.ranking ?? '0').toString();
    brand.score = brandData.score ?? 0;
    brand.stateScore = brandData.stateScore ?? 0;
    brand.scoreDay = brandData.scoreDay ?? 0;
    brand.stateScoreDay = brandData.stateScoreDay ?? 0;
    brand.scoreWeek = brandData.scoreWeek ?? 0;
    brand.stateScoreWeek = brandData.stateScoreWeek ?? 0;
    brand.rankingWeek = brandData.rankingWeek ?? 0;
    brand.scoreMonth = brandData.scoreMonth ?? 0;
    brand.stateScoreMonth = brandData.stateScoreMonth ?? 0;
    brand.rankingMonth = brandData.rankingMonth ?? 0;
    brand.bonusPoints = brandData.bonusPoints ?? 0;
    brand.banned = brandData.banned ?? 0;

    // Save to database
    await this.brandRepository.save(brand);
    console.log(
      `✅ Saved brand: ${brandData.name} (profile: ${brand.profile || 'none'}, channel: ${brand.channel})`,
    );

    return true;
  }

  /**
   * Gets existing category or creates new one
   */
  private async getOrCreateCategory(categoryName: string): Promise<Category> {
    let category = await this.categoryRepository.findOne({
      where: { name: categoryName },
    });

    if (!category) {
      category = this.categoryRepository.create({
        name: categoryName,
      });

      await this.categoryRepository.save(category);
      logger.log(`📁 Created category: ${categoryName}`);
    }

    return category;
  }

  /**
   * Generates Warpcast URL from brand data
   */
  private generateWarpcastUrl(brandData: BrandSeedData): string {
    if (brandData.profile && brandData.profile.startsWith('@')) {
      return `https://farcaster.xyz/${brandData.profile}`;
    }
    return brandData.url || `https://farcaster.xyz/${brandData.profile}`;
  }

  /**
   * Generates a default URL when none is provided
   */
  private generateDefaultUrl(profile: string): string {
    // Use Warpcast profile as the primary URL if no website exists
    return `https://farcaster.xyz/${profile}`;
  }

  /**
   * Validates URL format
   */
  private isValidUrl(url: string): boolean {
    try {
      new URL(url);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Gets database statistics
   */
  async getStats(): Promise<{
    totalBrands: number;
    totalCategories: number;
    brandsByCategory: Record<string, number>;
    brandsByQueryType: Record<string, number>;
  }> {
    const brands = await this.brandRepository.find({ relations: ['category'] });
    const totalCategories = await this.categoryRepository.count();

    const brandsByCategory = brands.reduce(
      (acc, brand) => {
        const categoryName = brand.category?.name || 'Uncategorized';
        acc[categoryName] = (acc[categoryName] || 0) + 1;
        return acc;
      },
      {} as Record<string, number>,
    );

    const brandsByQueryType = brands.reduce(
      (acc, brand) => {
        const type = brand.queryType === 0 ? 'Channel' : 'Profile';
        acc[type] = (acc[type] || 0) + 1;
        return acc;
      },
      {} as Record<string, number>,
    );

    return {
      totalBrands: brands.length,
      totalCategories,
      brandsByCategory,
      brandsByQueryType,
    };
  }

  /**
   * Clears all brands (use with caution!)
   */
  async clearAllBrands(): Promise<number> {
    const deleteResult = await this.brandRepository.delete({});
    const deletedCount = deleteResult.affected || 0;
    logger.log(`🗑️  Cleared ${deletedCount} brands from database`);
    return deletedCount;
  }

  /**
   * Preview what would happen during seeding without actually doing it
   */
  async previewSeeding(): Promise<{
    totalBrands: number;
    newBrands: string[];
    existingBrands: string[];
    missingChannels: string[];
    missingUrls: string[];
    categories: string[];
  }> {
    const seedData = await this.readSeedFile();
    const preview = {
      totalBrands: seedData.length,
      newBrands: [] as string[],
      existingBrands: [] as string[],
      missingChannels: [] as string[],
      missingUrls: [] as string[],
      categories: [] as string[],
    };

    for (const brandData of seedData) {
      // Check if exists
      const exists = await this.brandRepository.findOne({
        where: { name: brandData.name },
      });

      if (exists) {
        preview.existingBrands.push(brandData.name);
      } else {
        preview.newBrands.push(brandData.name);
      }

      // Check for missing channels
      if (!brandData.channel) {
        preview.missingChannels.push(brandData.name);
      }

      // Check for missing URLs
      if (!brandData.url) {
        preview.missingUrls.push(brandData.name);
      }

      // Track categories
      if (!preview.categories.includes(brandData.category)) {
        preview.categories.push(brandData.category);
      }
    }

    return preview;
  }
}
