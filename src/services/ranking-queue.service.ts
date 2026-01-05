import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Brand } from '../models';
import { logger } from '../main';

@Injectable()
export class RankingQueueService {
  private updateQueue = new Set<number>(); // Brand IDs that need ranking updates
  private isProcessing = false;
  private readonly BATCH_DELAY = 10000; // 10 seconds delay before processing
  private batchTimeout: NodeJS.Timeout | null = null;

  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
  ) {}

  /**
   * Queue a brand for ranking update (called from indexer)
   */
  queueRankingUpdate(brandId: number): void {
    this.updateQueue.add(brandId);
    this.scheduleUpdate();
  }

  /**
   * Schedule a batch update with debouncing
   */
  private scheduleUpdate(): void {
    if (this.batchTimeout) {
      clearTimeout(this.batchTimeout);
    }

    this.batchTimeout = setTimeout(() => {
      this.processBatch();
    }, this.BATCH_DELAY);
  }

  /**
   * Process queued ranking updates
   */
  private async processBatch(): Promise<void> {
    if (this.isProcessing || this.updateQueue.size === 0) {
      return;
    }

    this.isProcessing = true;
    const queuedBrands = Array.from(this.updateQueue);
    this.updateQueue.clear();

    try {
      logger.log(`🏆 [RANKING] Processing ranking updates for ${queuedBrands.length} brands`);

      // Get all brands sorted by score
      const brands = await this.brandRepository
        .createQueryBuilder('brand')
        .select(['brand.id', 'brand.ranking'])
        .orderBy('brand.score', 'DESC')
        .getMany();

      // Update rankings for affected brands only
      const updates = [];
      for (let i = 0; i < brands.length; i++) {
        const brand = brands[i];
        const newRanking = (i + 1).toString();
        
        if (queuedBrands.includes(brand.id) && brand.ranking !== newRanking) {
          updates.push(
            this.brandRepository.update(brand.id, { ranking: newRanking })
          );
        }
      }

      await Promise.all(updates);
      logger.log(`✅ [RANKING] Updated rankings for ${updates.length} brands`);
      
    } catch (error) {
      logger.error(`❌ [RANKING] Error updating rankings:`, error);
    } finally {
      this.isProcessing = false;
    }
  }
}