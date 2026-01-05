import { Injectable } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Brand } from '../../../models';
import { logger } from '../../../main';

@Injectable()
export class BrandRankingService {
  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
  ) {}

  /**
   * Updates global brand rankings every hour at minute 0
   * Cron format: second minute hour day month dayOfWeek
   * 0 0 * * * * = Every hour at minute 0
   */
  @Cron(CronExpression.EVERY_HOUR)
  async updateGlobalRankingsScheduled(): Promise<void> {
    logger.log('🏆 [SCHEDULER] Starting scheduled global brand rankings update...');

    try {
      await this.updateGlobalRankings();
      logger.log('✅ [SCHEDULER] Scheduled ranking update completed successfully');
    } catch (error) {
      logger.error('❌ [SCHEDULER] Scheduled ranking update failed:', error);
    }
  }

  /**
   * Updates global brand rankings based on current scores
   * This is the core logic from the script, but integrated into the service
   */
  async updateGlobalRankings(): Promise<{
    totalBrands: number;
    brandsUpdated: number;
    errors: number;
  }> {
    const startTime = Date.now();
    logger.log('🏆 [RANKING] Starting global brand rankings update...');

    try {
      // Get all brands sorted by score (highest first)
      const brands = await this.brandRepository
        .createQueryBuilder('brand')
        .select(['brand.id', 'brand.name', 'brand.score', 'brand.ranking'])
        .orderBy('brand.score', 'DESC')
        .getMany();

      if (brands.length === 0) {
        logger.log('ℹ️ [RANKING] No brands found in database');
        return { totalBrands: 0, brandsUpdated: 0, errors: 0 };
      }

      logger.log(`📊 [RANKING] Found ${brands.length} brands to process`);

      // Calculate new rankings and find brands that need updates
      const brandsToUpdate: Array<{ id: number; newRanking: string; name: string }> = [];
      
      for (let i = 0; i < brands.length; i++) {
        const brand = brands[i];
        const newRanking = (i + 1).toString();
        
        if (brand.ranking !== newRanking) {
          brandsToUpdate.push({
            id: brand.id,
            newRanking,
            name: brand.name,
          });
        }
      }

      logger.log(`📈 [RANKING] Analysis: ${brandsToUpdate.length}/${brands.length} brands need ranking updates`);

      if (brandsToUpdate.length === 0) {
        logger.log('✅ [RANKING] All brand rankings are already correct!');
        return { totalBrands: brands.length, brandsUpdated: 0, errors: 0 };
      }

      // Log top 5 changes for monitoring
      if (brandsToUpdate.length > 0) {
        logger.log('🏆 [RANKING] Top 5 ranking changes:');
        brandsToUpdate.slice(0, 5).forEach((brand) => {
          const currentRanking = brands.find(b => b.id === brand.id)?.ranking || '0';
          logger.log(`  • ${brand.name}: #${currentRanking} → #${brand.newRanking}`);
        });
      }

      // Update rankings in batches for better performance
      let updated = 0;
      let errors = 0;
      const batchSize = 50;

      for (let i = 0; i < brandsToUpdate.length; i += batchSize) {
        const batch = brandsToUpdate.slice(i, i + batchSize);
        
        // Process batch updates in parallel
        const updatePromises = batch.map(async (brand) => {
          try {
            await this.brandRepository.update(brand.id, {
              ranking: brand.newRanking,
            });
            updated++;
            return { success: true, brand: brand.name };
          } catch (error) {
            errors++;
            logger.error(`❌ [RANKING] Error updating brand ${brand.name} (ID: ${brand.id}):`, error);
            return { success: false, brand: brand.name, error };
          }
        });

        await Promise.all(updatePromises);

        // Log progress for large batches
        if (brandsToUpdate.length > batchSize) {
          const progress = ((i + batchSize) / brandsToUpdate.length * 100).toFixed(1);
          logger.log(`📈 [RANKING] Progress: ${Math.min(i + batchSize, brandsToUpdate.length)}/${brandsToUpdate.length} (${progress}%)`);
        }
      }

      const duration = Date.now() - startTime;
      logger.log(`🎉 [RANKING] Global ranking update completed in ${duration}ms`);
      logger.log(`📊 [RANKING] Summary: ${updated} updated, ${errors} errors, ${brands.length - brandsToUpdate.length} already correct`);

      if (errors > 0) {
        logger.warn(`⚠️ [RANKING] ${errors} errors occurred during ranking update`);
      }

      return {
        totalBrands: brands.length,
        brandsUpdated: updated,
        errors,
      };

    } catch (error) {
      logger.error('❌ [RANKING] Fatal error during ranking update:', error);
      throw error;
    }
  }

  /**
   * Manual trigger for ranking update (can be called from admin endpoint)
   */
  async triggerManualUpdate(): Promise<{
    success: boolean;
    totalBrands: number;
    brandsUpdated: number;
    errors: number;
  }> {
    try {
      logger.log('🏆 [RANKING] Manual ranking update triggered');
      const result = await this.updateGlobalRankings();
      return {
        success: true,
        ...result,
      };
    } catch (error) {
      logger.error('❌ [RANKING] Manual ranking update failed:', error);
      return {
        success: false,
        totalBrands: 0,
        brandsUpdated: 0,
        errors: 1,
      };
    }
  }
}