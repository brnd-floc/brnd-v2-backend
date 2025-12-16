// Create a new service: src/core/brand/services/brand-scheduler.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, MoreThan } from 'typeorm';
import { Brand, UserBrandVotes } from '../../../models';
import { FarcasterNotificationService } from '../../notification/services';
import { getConfig } from '../../../security/config';
import {
  devLog,
  debugLog,
  warnLog,
  errorLog,
  criticalLog,
} from '../../../utils/logger.utils';

@Injectable()
export class BrandSchedulerService {
  private readonly logger = new Logger(BrandSchedulerService.name);
  private readonly miniappUrl = getConfig().notifications.miniappUrl;

  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
    private readonly notificationService: FarcasterNotificationService,
  ) {}

  /**
   * Unified cron job that runs every day at midnight UTC
   * Handles day, week, and month endings with priority: Month > Week > Day
   */
  @Cron('0 1 * * *', { timeZone: 'UTC' }) // Every day at 1am UTC
  async handlePeriodEnd() {
    const now = new Date();
    console.log(
      '🕐 [CRON] Daily period end cron job triggered at:',
      now.toISOString(),
    );
    criticalLog(this.logger, `PERIOD END: Processing at ${now.toISOString()}`);

    try {
      // Check what periods are ending
      const isEndOfMonth = this.isEndOfMonth(now);
      const isEndOfWeek = this.isEndOfWeek(now);
      // Day always ends (it's a new day)

      debugLog(
        this.logger,
        `PERIOD END: Month: ${isEndOfMonth}, Week: ${isEndOfWeek}, Day: true`,
      );

      // Get top brands BEFORE resetting scores
      let topMonthlyBrands: Brand[] = [];
      let topWeeklyBrands: Brand[] = [];
      let topDailyBrands: Brand[] = [];

      if (isEndOfMonth) {
        topMonthlyBrands = await this.getTopBrands('month', 3);
        devLog(
          this.logger,
          `MONTH Top brands: ${topMonthlyBrands.map((b) => b.name).join(', ')}`,
        );
      }

      if (isEndOfWeek) {
        topWeeklyBrands = await this.getTopBrands('week', 3);
        devLog(
          this.logger,
          `WEEK Top brands: ${topWeeklyBrands.map((b) => b.name).join(', ')}`,
        );
      }

      // Always get daily top brands
      topDailyBrands = await this.getTopBrands('day', 3);
      devLog(
        this.logger,
        `DAY Top brands: ${topDailyBrands.map((b) => b.name).join(', ')}`,
      );

      // Reset scores and send notifications
      if (isEndOfMonth) {
        await this.resetMonthlyScores();
        await this.sendMonthlyNotification(topMonthlyBrands);
      }

      if (isEndOfWeek) {
        await this.resetWeeklyScores();
        await this.sendWeeklyNotification(topWeeklyBrands);
      }

      // Always reset daily scores
      await this.resetDailyScores();
      await this.sendDailyNotification(topDailyBrands);
    } catch (error) {
      errorLog(this.logger, 'PERIOD END Error:', error);
    }
  }

  /**
   * Check if current date is end of month (last day at midnight UTC)
   */
  private isEndOfMonth(date: Date): boolean {
    const tomorrow = new Date(date);
    tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
    return tomorrow.getUTCDate() === 1; // Next day is 1st of month
  }

  /**
   * Check if current date is end of week (Friday at midnight UTC = Saturday 00:00 UTC)
   * Week runs from Saturday 00:00 UTC to Friday 23:59 UTC
   */
  private isEndOfWeek(date: Date): boolean {
    const dayOfWeek = date.getUTCDay();
    return dayOfWeek === 6; // Saturday 00:00 UTC = end of week (Friday just ended)
  }

  /**
   * Get top N brands for a given period
   */
  private async getTopBrands(
    period: 'day' | 'week' | 'month',
    limit: number,
  ): Promise<Brand[]> {
    const scoreField =
      period === 'day'
        ? 'scoreDay'
        : period === 'week'
          ? 'scoreWeek'
          : 'scoreMonth';

    return await this.brandRepository.find({
      where: { banned: 0 },
      order: { [scoreField]: 'DESC' },
      take: limit,
    });
  }

  /**
   * Reset daily scores
   */
  private async resetDailyScores(): Promise<void> {
    const updateResult = await this.brandRepository.update(
      {},
      {
        scoreDay: 0,
        stateScoreDay: 0,
      },
    );
    this.logger.log(
      `✅ Reset daily scores for ${updateResult.affected} brands`,
    );
  }

  /**
   * Reset weekly scores
   */
  private async resetWeeklyScores(): Promise<void> {
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
  }

  /**
   * Reset monthly scores
   */
  private async resetMonthlyScores(): Promise<void> {
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
  }

  /**
   * Send daily notification
   */
  private async sendDailyNotification(brands: Brand[]): Promise<void> {
    const title = 'Vote for farcaster brands!'; // 26 chars - compliant
    const body = this.notificationService.formatPodiumMessage(brands);
    const notificationId = `daily-${new Date().toISOString().split('T')[0]}`;

    await this.notificationService.sendNotificationToAllUsers(
      title,
      body,
      this.miniappUrl,
      notificationId,
    );
  }

  /**
   * Send weekly notification
   */
  private async sendWeeklyNotification(brands: Brand[]): Promise<void> {
    const title = 'Top brands of the week';
    const body = this.notificationService.formatPodiumMessage(brands);
    const now = new Date();
    const weekEnd = new Date(now);
    weekEnd.setUTCDate(now.getUTCDate() - 1); // Yesterday (Friday)
    const notificationId = `weekly-${weekEnd.toISOString().split('T')[0]}`;

    await this.notificationService.sendNotificationToAllUsers(
      title,
      body,
      this.miniappUrl,
      notificationId,
    );
  }

  /**
   * Send monthly notification
   */
  private async sendMonthlyNotification(brands: Brand[]): Promise<void> {
    const title = 'Top brands of the month';
    const body = this.notificationService.formatPodiumMessage(brands);
    const now = new Date();
    const lastMonth = new Date(now.getUTCFullYear(), now.getUTCMonth() - 1, 1);
    const notificationId = `monthly-${lastMonth.getUTCFullYear()}-${String(lastMonth.getUTCMonth() + 1).padStart(2, '0')}`;

    await this.notificationService.sendNotificationToAllUsers(
      title,
      body,
      this.miniappUrl,
      notificationId,
    );
  }

  /**
   * Daily reminder at 5 PM UTC for users who haven't voted
   * Shows top 3 brands of the day so far
   */
  @Cron('0 17 * * *', { timeZone: 'UTC' }) // Every day at 5 PM UTC
  async sendDailyVoteReminder() {
    console.log(
      '🔔 [CRON] Daily vote reminder cron job triggered at:',
      new Date().toISOString(),
    );
    criticalLog(this.logger, 'REMINDER: Sending daily vote reminder');

    try {
      // Get top 3 brands of the day so far
      const topDailyBrands = await this.getTopBrands('day', 3);
      devLog(
        this.logger,
        `REMINDER Top brands: ${topDailyBrands.map((b) => b.name).join(', ')}`,
      );

      const title = 'Vote reminder - top brands!'; // 28 chars - compliant
      const body = this.notificationService.formatPodiumMessage(topDailyBrands);
      const today = new Date().toISOString().split('T')[0];
      const notificationId = `reminder-${today}`;

      await this.notificationService.sendNotificationToNonVoters(
        title,
        body,
        this.miniappUrl,
        notificationId,
      );
    } catch (error) {
      errorLog(this.logger, 'REMINDER Error:', error);
    }
  }

  /**
   * Check system health every hour
   * Detect if resets are working properly
   */
  @Cron(CronExpression.EVERY_HOUR)
  async healthCheck() {
    console.log(
      '💊 [CRON] Health check cron job triggered at:',
      new Date().toISOString(),
    );
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
      debugLog(
        this.logger,
        `HEALTH: Recent votes: ${recentVotes}, Sample weekly score: ${sampleBrand?.scoreWeek || 0}`,
      );

      // Alert if something looks wrong
      if (sampleBrand && recentVotes > 0) {
        const maxExpectedWeekly = recentVotes * 60; // Conservative estimate
        if (sampleBrand.scoreWeek > maxExpectedWeekly * 3) {
          warnLog(
            this.logger,
            `HEALTH: Weekly scores high. Current: ${sampleBrand.scoreWeek}, Expected max: ${maxExpectedWeekly}`,
          );
        }
      }
    } catch (error) {
      errorLog(this.logger, 'HEALTH Error:', error);
    }
  }

  /**
   * Send hourly system status notification to admin
   * Runs every hour at 5 minutes past the hour
   */
  @Cron('5 * * * *', { timeZone: 'UTC' })
  async sendAdminStatusNotification() {
    const now = new Date();
    console.log(
      '🔔 [CRON] Admin status notification triggered at:',
      now.toISOString(),
    );

    try {
      const title = 'SYSTEM STATUS'; // 13 chars - compliant
      const body = 'SYSTEM IS WORKING FINE BOSS'; // 27 chars - compliant
      const notificationId = `admin-status-${now.getTime()}`;
      const adminFid = 16098; // Your FID

      const result =
        await this.notificationService.sendNotificationToSpecificFid(
          adminFid,
          title,
          body,
          this.miniappUrl,
          notificationId,
        );

      if (result.sent) {
        this.logger.log(`✅ Admin status notification sent: ${result.message}`);
      } else {
        this.logger.warn(
          `⚠️ Admin status notification failed: ${result.message}`,
        );
      }
    } catch (error) {
      errorLog(this.logger, 'ADMIN STATUS Error:', error);
    }
  }
}
