// src/core/notification/services/farcaster-notification.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Not, IsNull } from 'typeorm';
import { User, UserBrandVotes } from '../../../models';
import { Brand } from '../../../models';
import {
  devLog,
  debugLog,
  warnLog,
  errorLog,
  criticalLog,
} from '../../../utils/logger.utils';

@Injectable()
export class FarcasterNotificationService {
  private readonly logger = new Logger(FarcasterNotificationService.name);
  private readonly NOTIFICATION_URL =
    'https://api.farcaster.xyz/v1/frame-notifications';

  // Rate limiting constants
  private readonly RATE_LIMIT_INTERVAL = 30 * 1000; // 30 seconds
  private readonly DAILY_LIMIT = 100;
  private readonly userLastNotification = new Map<number, number>();
  private readonly userDailyCount = new Map<string, number>();

  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
  ) {}

  /**
   * Validate notification content against Farcaster limits
   */
  private validateNotificationContent(
    title: string,
    body: string,
    notificationId: string,
  ): void {
    if (title.length > 32) {
      throw new Error(
        `Title exceeds 32 character limit: ${title.length} chars`,
      );
    }
    if (body.length > 128) {
      throw new Error(`Body exceeds 128 character limit: ${body.length} chars`);
    }
    if (notificationId.length > 128) {
      throw new Error(
        `NotificationId exceeds 128 character limit: ${notificationId.length} chars`,
      );
    }
  }

  /**
   * Check if user can receive notification based on rate limits
   */
  private canSendToUser(userId: number): boolean {
    const now = Date.now();
    const today = new Date().toDateString();

    // Check 30-second rate limit
    const lastNotification = this.userLastNotification.get(userId);
    if (lastNotification && now - lastNotification < this.RATE_LIMIT_INTERVAL) {
      return false;
    }

    // Check daily limit
    const dailyKey = `${userId}-${today}`;
    const dailyCount = this.userDailyCount.get(dailyKey) || 0;
    if (dailyCount >= this.DAILY_LIMIT) {
      return false;
    }

    return true;
  }

  /**
   * Record notification sent for rate limiting
   */
  private recordNotificationSent(userId: number): void {
    const now = Date.now();
    const today = new Date().toDateString();

    this.userLastNotification.set(userId, now);

    const dailyKey = `${userId}-${today}`;
    const dailyCount = this.userDailyCount.get(dailyKey) || 0;
    this.userDailyCount.set(dailyKey, dailyCount + 1);
  }

  /**
   * Send notification to all users with notification tokens
   * Uses notificationId for idempotency (Farcaster deduplicates by (FID, notificationId))
   */
  async sendNotificationToAllUsers(
    title: string,
    body: string,
    targetUrl: string,
    notificationId: string,
  ): Promise<{ sent: number; failed: number; rateLimited: number }> {
    // Validate content against Farcaster limits
    this.validateNotificationContent(title, body, notificationId);

    criticalLog(
      this.logger,
      `Sending notification: ${title} (ID: ${notificationId})`,
    );

    // Get all users with notification tokens
    const users = await this.userRepository.find({
      where: {
        notificationToken: Not(IsNull()),
        notificationsEnabled: true,
      },
      select: ['id', 'fid', 'notificationToken'],
    });

    if (users.length === 0) {
      warnLog(this.logger, 'No users with notification tokens found');
      return { sent: 0, failed: 0, rateLimited: 0 };
    }

    debugLog(this.logger, `Found ${users.length} users to notify`);

    // Filter users based on rate limits
    const eligibleUsers = users.filter((user) => this.canSendToUser(user.id));
    const rateLimited = users.length - eligibleUsers.length;

    if (rateLimited > 0) {
      debugLog(
        this.logger,
        `${rateLimited} users rate-limited, sending to ${eligibleUsers.length}`,
      );
    }

    if (eligibleUsers.length === 0) {
      warnLog(this.logger, 'No eligible users after rate limiting');
      return { sent: 0, failed: 0, rateLimited };
    }

    // Batch notifications (up to 100 per request as per Farcaster docs)
    const batchSize = 100;
    let sent = 0;
    let failed = 0;

    for (let i = 0; i < eligibleUsers.length; i += batchSize) {
      const batch = eligibleUsers.slice(i, i + batchSize);
      const tokens = batch.map((u) => u.notificationToken).filter(Boolean);

      try {
        const response = await fetch(this.NOTIFICATION_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            notificationId, // Same ID for all users - Farcaster deduplicates by (FID, notificationId)
            title,
            body,
            targetUrl,
            tokens, // Array of tokens for batch sending
          }),
        });

        if (!response.ok) {
          const errorText = await response.text();
          this.logger.error(
            `❌ Failed to send notification batch: ${response.status} ${errorText}`,
          );
          failed += batch.length;
        } else {
          const result = await response.json();
          const batchSent = result.sent || batch.length;
          sent += batchSent;

          // Record notifications for rate limiting
          batch.forEach((user) => this.recordNotificationSent(user.id));

          this.logger.log(
            `✅ Sent batch: ${batchSent}/${batch.length} notifications`,
          );
        }
      } catch (error) {
        errorLog(this.logger, `Error sending notification batch:`, error);
        failed += batch.length;
      }
    }

    this.logger.log(
      `📊 Notification summary: ${sent} sent, ${failed} failed, ${rateLimited} rate-limited out of ${users.length} users`,
    );

    return { sent, failed, rateLimited };
  }

  /**
   * Send notification only to users who haven't voted today
   */
  async sendNotificationToNonVoters(
    title: string,
    body: string,
    targetUrl: string,
    notificationId: string,
  ): Promise<{ sent: number; failed: number; rateLimited: number }> {
    // Validate content against Farcaster limits
    this.validateNotificationContent(title, body, notificationId);

    this.logger.log(
      `📤 Sending reminder to non-voters: ${title} (ID: ${notificationId})`,
    );

    // Calculate today's day number (unix timestamp / 86400)
    const now = Math.floor(Date.now() / 1000);
    const todayDay = Math.floor(now / 86400);

    // Get all users with notification tokens
    const allUsers = await this.userRepository.find({
      where: {
        notificationToken: Not(IsNull()),
        notificationsEnabled: true,
      },
      select: ['id', 'fid', 'notificationToken'],
    });

    if (allUsers.length === 0) {
      this.logger.log('⚠️ No users with notification tokens found');
      return { sent: 0, failed: 0, rateLimited: 0 };
    }

    // Get users who have voted today
    const usersWhoVotedToday = await this.userBrandVotesRepository.find({
      where: {
        day: todayDay,
      },
      relations: ['user'],
      select: ['user'],
    });

    const votedUserIds = new Set(
      usersWhoVotedToday.map((vote) => vote.user.id),
    );

    // Filter to only users who haven't voted today
    const nonVoters = allUsers.filter((user) => !votedUserIds.has(user.id));

    // Apply rate limiting
    const eligibleNonVoters = nonVoters.filter((user) =>
      this.canSendToUser(user.id),
    );
    const rateLimited = nonVoters.length - eligibleNonVoters.length;

    if (nonVoters.length === 0) {
      this.logger.log('✅ All users have voted today, no reminders needed');
      return { sent: 0, failed: 0, rateLimited: 0 };
    }

    if (eligibleNonVoters.length === 0) {
      this.logger.log(`⚠️ All ${nonVoters.length} non-voters are rate-limited`);
      return { sent: 0, failed: 0, rateLimited };
    }

    this.logger.log(
      `📋 Found ${eligibleNonVoters.length} eligible non-voters (${rateLimited} rate-limited) out of ${nonVoters.length} total non-voters`,
    );

    // Batch notifications (up to 100 per request as per Farcaster docs)
    const batchSize = 100;
    let sent = 0;
    let failed = 0;

    for (let i = 0; i < eligibleNonVoters.length; i += batchSize) {
      const batch = eligibleNonVoters.slice(i, i + batchSize);
      const tokens = batch.map((u) => u.notificationToken).filter(Boolean);

      try {
        const response = await fetch(this.NOTIFICATION_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            notificationId, // Same ID for all users - Farcaster deduplicates by (FID, notificationId)
            title,
            body,
            targetUrl,
            tokens,
          }),
        });

        if (!response.ok) {
          const errorText = await response.text();
          this.logger.error(
            `❌ Failed to send notification batch: ${response.status} ${errorText}`,
          );
          failed += batch.length;
        } else {
          const result = await response.json();
          const batchSent = result.sent || batch.length;
          sent += batchSent;

          // Record notifications for rate limiting
          batch.forEach((user) => this.recordNotificationSent(user.id));

          this.logger.log(
            `✅ Sent batch: ${batchSent}/${batch.length} notifications`,
          );
        }
      } catch (error) {
        errorLog(this.logger, `Error sending notification batch:`, error);
        failed += batch.length;
      }
    }

    this.logger.log(
      `📊 Reminder summary: ${sent} sent, ${failed} failed, ${rateLimited} rate-limited out of ${nonVoters.length} non-voters`,
    );

    return { sent, failed, rateLimited };
  }

  /**
   * Format podium message for top 3 brands with character limit compliance
   */
  formatPodiumMessage(
    brands: Brand[],
    period: 'day' | 'week' | 'month',
  ): string {
    if (brands.length === 0) {
      return 'No brands this period.';
    }

    const periodText =
      period === 'day'
        ? 'this day'
        : period === 'week'
          ? 'this week'
          : 'the month';

    const podium = brands
      .slice(0, 3)
      .map((brand, index) => {
        const medal = ['🥇', '🥈', '🥉'][index];
        const brandName =
          brand.name.length > 20
            ? brand.name.substring(0, 17) + '...'
            : brand.name;
        return `Top brands of ${periodText}: ${medal} ${brandName}`;
      })
      .join('\n');

    // Ensure the message doesn't exceed 128 chars
    if (podium.length > 128) {
      // Fallback: just show count
      return `Top brands of ${periodText}: ${brands
        .slice(0, 3)
        .map((_, i) => ['🥇', '🥈', '🥉'][i])
        .join(' ')}`;
    }

    return podium;
  }

  /**
   * Send notification to a specific user by FID
   */
  async sendNotificationToSpecificFid(
    fid: number,
    title: string,
    body: string,
    targetUrl: string = 'https://brnd.land',
    notificationId: string,
  ): Promise<{ sent: boolean; message: string }> {
    // Validate content against Farcaster limits
    this.validateNotificationContent(title, body, notificationId);

    this.logger.log(
      `📤 Sending notification to FID ${fid}: ${title} (ID: ${notificationId})`,
    );

    // Get the specific user
    const user = await this.userRepository.findOne({
      where: {
        fid,
        notificationToken: Not(IsNull()),
        notificationsEnabled: true,
      },
      select: ['id', 'fid', 'notificationToken'],
    });

    if (!user) {
      const message = `User with FID ${fid} not found or doesn't have notifications enabled`;
      this.logger.warn(`⚠️ ${message}`);
      return { sent: false, message };
    }

    // Check rate limiting
    if (!this.canSendToUser(user.id)) {
      const message = `User FID ${fid} is rate-limited`;
      this.logger.warn(`⚠️ ${message}`);
      return { sent: false, message };
    }

    try {
      const response = await fetch(this.NOTIFICATION_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          notificationId,
          title,
          body,
          targetUrl,
          tokens: [user.notificationToken],
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        const message = `Failed to send notification: ${response.status} ${errorText}`;
        this.logger.error(`❌ ${message}`);
        return { sent: false, message };
      }

      // Record notification for rate limiting
      this.recordNotificationSent(user.id);

      const message = `Notification sent successfully to FID ${fid}`;
      this.logger.log(`✅ ${message}`);
      return { sent: true, message };
    } catch (error) {
      const message = `Error sending notification to FID ${fid}: ${error.message}`;
      this.logger.error(`❌ ${message}`, error);
      return { sent: false, message };
    }
  }

  /**
   * Store notification token for a user (called from webhook)
   */
  async storeNotificationToken(
    fid: number,
    notificationToken: string | null,
  ): Promise<void> {
    try {
      if (notificationToken) {
        // Frame added - store token and enable notifications
        await this.userRepository.update(
          { fid },
          {
            notificationToken,
            notificationsEnabled: true,
          },
        );
        this.logger.log(`✅ Stored notification token for FID: ${fid}`);
      } else {
        // Frame removed - clear token and disable notifications
        await this.userRepository.update(
          { fid },
          {
            notificationToken: null,
            notificationsEnabled: false,
          },
        );
        this.logger.log(`🗑️ Cleared notification token for FID: ${fid}`);
      }
    } catch (error) {
      this.logger.error(
        `❌ Failed to update notification token for FID: ${fid}`,
        error,
      );
      throw error;
    }
  }
}
