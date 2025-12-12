// src/core/notification/services/neynar-notification.service.ts

import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { NeynarAPIClient } from '@neynar/nodejs-sdk';
import { User } from '../../../models';
import { getConfig } from '../../../security/config';

@Injectable()
export class NeynarNotificationService {
  private readonly logger = new Logger(NeynarNotificationService.name);
  private readonly config = getConfig();
  private readonly neynarClient: NeynarAPIClient;

  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
  ) {
    if (!process.env.NEYNAR_API_KEY) {
      throw new Error('NEYNAR_API_KEY is required for notification service');
    }

    this.neynarClient = new NeynarAPIClient({
      apiKey: process.env.NEYNAR_API_KEY,
    });
  }

  /**
   * Sends daily vote reminder to all users at start of UTC day
   * Simple broadcast to all users who have the miniapp added
   */
  // async sendDailyVoteReminder(): Promise<void> {
  //   try {
  //     this.logger.log('Sending daily vote reminder notification');

  //     const notification = {
  //       title: '🗳️ Daily Vote Time!',
  //       body: 'New day, new votes! Choose your top 3 brands and earn points.',
  //       target_url: 'https://brnd.land',
  //     };

  //     // const response = await this.neynarClient.publishFrameNotifications({
  //     //   targetFids: [], // Empty array targets all users with notifications enabled
  //     //   notification,
  //     // });

  //     this.logger.log(`Daily reminder sent successfully:`, response);
  //   } catch (error) {
  //     this.logger.error('Failed to send daily vote reminder:', error);
  //   }
  // }

  /**
   * Sends evening reminder only to users who haven't voted today
   * Requires checking against today's votes before sending
   */
  // async sendEveningReminderToNonVoters(): Promise<void> {
  //   try {
  //     this.logger.log("Sending evening reminders to users who haven't voted");

  //     const today = new Date().toISOString().split('T')[0];
  //     // Create date range for better performance (avoids DATE() function)
  //     const startOfDay = new Date(today + ' 00:00:00');
  //     const endOfDay = new Date(today + ' 23:59:59');

  //     // Find users who haven't voted today - optimized query
  //     const usersWhoHaventVoted = await this.userRepository
  //       .createQueryBuilder('user')
  //       .leftJoin(
  //         'user.userBrandVotes',
  //         'vote',
  //         'vote.date >= :startOfDay AND vote.date <= :endOfDay',
  //         {
  //           startOfDay,
  //           endOfDay,
  //         },
  //       )
  //       .where('vote.transactionHash IS NULL') // No vote today
  //       .select(['user.fid'])
  //       .getMany();

  //     if (usersWhoHaventVoted.length === 0) {
  //       this.logger.log(
  //         'No users need evening reminders - everyone has voted!',
  //       );
  //       return;
  //     }

  //     const targetFids = usersWhoHaventVoted.map((user) => user.fid);

  //     const notification = {
  //       title: '⏰ Last Call to Vote!',
  //       body: "Don't miss out! Vote for your favorite brands before day ends.",
  //       target_url: 'https://brnd.land',
  //     };

  //     const response = await this.neynarClient.publishFrameNotifications({
  //       targetFids,
  //       notification,
  //     });

  //     this.logger.log(`Evening reminders sent successfully:`, {
  //       targetedUsers: targetFids.length,
  //       response,
  //     });
  //   } catch (error) {
  //     this.logger.error('Failed to send evening reminders:', error);
  //   }
  // }

  /**
   * Health check for the notification service
   * Verifies Neynar API connectivity
   */
  async healthCheck(): Promise<{ status: string; neynar: string }> {
    try {
      // Simple API call to verify connectivity
      await this.neynarClient.fetchNotificationTokens({ limit: 1 });
      return {
        status: 'healthy',
        neynar: 'connected',
      };
    } catch (error) {
      this.logger.error('Neynar health check failed:', error);
      return {
        status: 'unhealthy',
        neynar: 'disconnected',
      };
    }
  }
}
