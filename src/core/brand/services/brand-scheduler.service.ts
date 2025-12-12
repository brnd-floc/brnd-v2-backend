// Create a new service: src/core/brand/services/brand-scheduler.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, MoreThan } from 'typeorm';
import { Brand, UserBrandVotes } from '../../../models';

@Injectable()
export class BrandSchedulerService {
  private readonly logger = new Logger(BrandSchedulerService.name);

  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
  ) {}

  /**
   * Reset weekly scores every Friday at 3:00 PM Chile time (18:00 UTC)
   * Runs automatically, no API calls needed
   */
  @Cron('0 18 * * 5', { timeZone: 'UTC' }) // Every Friday at 18:00 UTC
  async resetWeeklyScores() {
    this.logger.log('🔄 Starting automatic weekly score reset...');

    try {
      // Reset all weekly scores to 0
      const updateResult = await this.brandRepository.update(
        {},
        {
          scoreWeek: 0,
          stateScoreWeek: 0,
          rankingWeek: 0,
        },
      );

      this.logger.log(
        `✅ Reset weekly scores for ${updateResult.affected} brands`,
      );

      // Emit event or notification here if needed
    } catch (error) {
      this.logger.error('❌ Failed to reset weekly scores:', error);
    }
  }

  /**
   * Reset monthly scores on 1st of every month at 9:00 AM UTC
   * Runs automatically, no API calls needed
   */
  @Cron('0 9 1 * *', { timeZone: 'UTC' }) // 1st day of month at 9:00 UTC
  async resetMonthlyScores() {
    this.logger.log('🔄 Starting automatic monthly score reset...');

    try {
      const updateResult = await this.brandRepository.update(
        {},
        {
          scoreMonth: 0,
          stateScoreMonth: 0,
          rankingMonth: 0,
        },
      );

      this.logger.log(
        `✅ Reset monthly scores for ${updateResult.affected} brands`,
      );

      // Notify admins or trigger winner announcement here
    } catch (error) {
      this.logger.error('❌ Failed to reset monthly scores:', error);
    }
  }

  /**
   * Check system health every hour
   * Detect if resets are working properly
   */
  @Cron(CronExpression.EVERY_HOUR)
  async healthCheck() {
    try {
      const now = new Date();
      const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

      // Get sample brand
      const sampleBrand = await this.brandRepository.findOne({
        where: { banned: 0 },
        order: { score: 'DESC' },
      });

      // Count recent votes
      const recentVotes = await this.userBrandVotesRepository.count({
        where: { date: MoreThan(oneWeekAgo) },
      });

      // Log health metrics
      this.logger.log(
        `📊 Health check - Recent votes: ${recentVotes}, Sample weekly score: ${sampleBrand?.scoreWeek || 0}`,
      );

      // Alert if something looks wrong
      if (sampleBrand && recentVotes > 0) {
        const maxExpectedWeekly = recentVotes * 60; // Conservative estimate
        if (sampleBrand.scoreWeek > maxExpectedWeekly * 3) {
          this.logger.warn(
            `⚠️ Weekly scores might need manual reset. Current: ${sampleBrand.scoreWeek}, Expected max: ${maxExpectedWeekly}`,
          );
        }
      }
    } catch (error) {
      this.logger.error('❌ Health check failed:', error);
    }
  }
}
