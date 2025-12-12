// src/core/notification/services/notification.scheduler.ts

import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { NeynarNotificationService } from './neynar-notification.service';

@Injectable()
export class NotificationScheduler {
  private readonly logger = new Logger(NotificationScheduler.name);

  constructor(
    private readonly neynarNotificationService: NeynarNotificationService,
  ) {}

  /**
   * Sends daily vote reminder at start of UTC day (12:00 AM UTC)
   * Simple notification to all users to start their daily voting
   */
  // @Cron('0 0 * * *', { name: 'dailyVoteReminder', timeZone: 'UTC' })
  // async sendDailyVoteReminder() {
  //   this.logger.log('Executing daily vote reminder (12:00 AM UTC)');
  //   try {
  //     await this.neynarNotificationService.sendDailyVoteReminder();
  //   } catch (error) {
  //     this.logger.error('Error in daily vote reminder cron job:', error);
  //   }
  // }

  /**
   * Sends evening reminder at 6:00 PM UTC only to users who haven't voted yet
   * Last chance reminder before day ends
   */
  // @Cron('0 18 * * *', { name: 'eveningVoteReminder', timeZone: 'UTC' })
  // async sendEveningVoteReminder() {
  //   this.logger.log('Executing evening vote reminder (6:00 PM UTC)');
  //   try {
  //     await this.neynarNotificationService.sendEveningReminderToNonVoters();
  //   } catch (error) {
  //     this.logger.error('Error in evening vote reminder cron job:', error);
  //   }
  // }
}
