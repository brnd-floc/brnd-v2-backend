import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Brand, UserBrandVotes } from '../../../models';

@Injectable()
export class BrandMetricsService {
  private readonly logger = new Logger(BrandMetricsService.name);

  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,

    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
  ) {}

  async updateBrandMetrics(brandId: number): Promise<void> {
    try {
      await Promise.all([
        this.updateUniqueVotersCount(brandId),
        this.updateBrandRanking(brandId),
      ]);

      this.logger.log(`Updated metrics for brand ${brandId}`);
    } catch (error) {
      this.logger.error(`Failed to update metrics for brand ${brandId}:`, error);
      throw error;
    }
  }

  async updateUniqueVotersCount(brandId: number): Promise<void> {
    const uniqueVotersCount = await this.userBrandVotesRepository
      .createQueryBuilder('vote')
      .select('COUNT(DISTINCT vote.userId)', 'count')
      .where(
        '(vote.brand1Id = :brandId OR vote.brand2Id = :brandId OR vote.brand3Id = :brandId)',
        { brandId },
      )
      .getRawOne();

    const count = parseInt(uniqueVotersCount?.count || '0');

    await this.brandRepository.update(brandId, {
      uniqueVotersCount: count,
    });

    this.logger.debug(`Updated uniqueVotersCount for brand ${brandId}: ${count}`);
  }

  async updateBrandRanking(brandId: number): Promise<void> {
    const brand = await this.brandRepository.findOne({
      where: { id: brandId },
      select: ['id', 'score'],
    });

    if (!brand) {
      this.logger.warn(`Brand ${brandId} not found for ranking update`);
      return;
    }

    const higherRankedCount = await this.brandRepository
      .createQueryBuilder('brand')
      .where('brand.banned = 0')
      .andWhere('brand.score > :brandScore', { brandScore: brand.score })
      .getCount();

    const currentRanking = higherRankedCount + 1;

    await this.brandRepository.update(brandId, {
      currentRanking,
    });

    this.logger.debug(`Updated ranking for brand ${brandId}: ${currentRanking}`);
  }

  async updateAllBrandMetrics(): Promise<void> {
    this.logger.log('Starting bulk update of all brand metrics...');

    const brands = await this.brandRepository.find({
      where: { banned: 0 },
      select: ['id'],
    });

    for (const brand of brands) {
      await this.updateBrandMetrics(brand.id);
    }

    this.logger.log(`Completed bulk update for ${brands.length} brands`);
  }
}