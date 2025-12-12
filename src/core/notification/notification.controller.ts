// src/core/notification/notification.controller.ts

import { Controller, Get, Post, HttpStatus, Res, Logger } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { Response } from 'express';
import { hasResponse, hasError } from '../../utils';
import { NeynarNotificationService } from './services';

@ApiTags('notification-service')
@Controller('notification-service')
export class NotificationController {
  private readonly logger = new Logger(NotificationController.name);

  constructor(
    private readonly neynarNotificationService: NeynarNotificationService,
  ) {}

  /**
   * Health check endpoint for Neynar notification system
   */
  @Get('/health')
  async healthCheck(@Res() res: Response): Promise<Response> {
    try {
      const health = await this.neynarNotificationService.healthCheck();

      return hasResponse(res, {
        ...health,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'healthCheck',
        error.message,
      );
    }
  }

  // Development-only endpoints for testing

  /**
   * Manual trigger for daily reminders - development environment only
   */
  // @Post('/dev/trigger-daily-reminder')
  // async triggerDailyReminder(@Res() res: Response): Promise<Response> {
  //   try {
  //     if (process.env.ENV === 'prod') {
  //       return hasError(
  //         res,
  //         HttpStatus.FORBIDDEN,
  //         'triggerDailyReminder',
  //         'Development endpoint not available in production',
  //       );
  //     }

  //     await this.neynarNotificationService.sendDailyVoteReminder();
  //     return hasResponse(res, {
  //       message: 'Daily reminder sent successfully via Neynar',
  //       timestamp: new Date().toISOString(),
  //     });
  //   } catch (error) {
  //     return hasError(
  //       res,
  //       HttpStatus.INTERNAL_SERVER_ERROR,
  //       'triggerDailyReminder',
  //       error.message,
  //     );
  //   }
  // }

  /**
   * Manual trigger for evening reminders - development environment only
   */
  // @Post('/dev/trigger-evening-reminder')
  // async triggerEveningReminder(@Res() res: Response): Promise<Response> {
  //   try {
  //     if (process.env.ENV === 'prod') {
  //       return hasError(
  //         res,
  //         HttpStatus.FORBIDDEN,
  //         'triggerEveningReminder',
  //         'Development endpoint not available in production',
  //       );
  //     }

  //     await this.neynarNotificationService.sendEveningReminderToNonVoters();
  //     return hasResponse(res, {
  //       message: 'Evening reminder sent successfully via Neynar',
  //       timestamp: new Date().toISOString(),
  //     });
  //   } catch (error) {
  //     return hasError(
  //       res,
  //       HttpStatus.INTERNAL_SERVER_ERROR,
  //       'triggerEveningReminder',
  //       error.message,
  //     );
  //   }
  // }
}
