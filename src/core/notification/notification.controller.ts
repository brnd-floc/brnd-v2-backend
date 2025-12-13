// src/core/notification/notification.controller.ts

import {
  Controller,
  Get,
  Post,
  Body,
  HttpStatus,
  Res,
  Logger,
} from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { Response } from 'express';
import { hasResponse, hasError } from '../../utils';
import { FarcasterNotificationService } from './services';

@ApiTags('notification-service')
@Controller('notification-service')
export class NotificationController {
  private readonly logger = new Logger(NotificationController.name);

  constructor(
    private readonly farcasterNotificationService: FarcasterNotificationService,
  ) {}

  /**
   * Health check endpoint for Farcaster notification system
   */
  @Get('/health')
  async healthCheck(@Res() res: Response): Promise<Response> {
    try {
      return hasResponse(res, {
        status: 'ok',
        message: 'Farcaster notification service is operational',
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

  /**
   * Farcaster webhook endpoint for frame added/removed actions
   * This endpoint receives notification tokens when users add/remove the frame
   */
  @Post('/farcaster-webhook')
  async farcasterWebhook(
    @Body() body: any,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      this.logger.log('📥 Received Farcaster webhook', JSON.stringify(body));

      // Extract data from webhook payload
      const { action, fid, notificationToken } = body;

      if (!action || !fid) {
        this.logger.warn('⚠️ Invalid webhook payload - missing action or fid');
        return hasError(
          res,
          HttpStatus.BAD_REQUEST,
          'farcasterWebhook',
          'Invalid payload: action and fid are required',
        );
      }

      if (action === 'frame_added' && notificationToken) {
        // Store the notification token for the user
        await this.farcasterNotificationService.storeNotificationToken(
          fid,
          notificationToken,
        );

        this.logger.log(`✅ Frame added for FID ${fid}, token stored`);

        return hasResponse(res, {
          status: 'success',
          message: 'Notification token stored successfully',
          fid,
          action,
        });
      } else if (action === 'frame_removed') {
        // Remove notification token (set to null)
        await this.farcasterNotificationService.storeNotificationToken(
          fid,
          null,
        );

        this.logger.log(`🗑️ Frame removed for FID ${fid}, token cleared`);

        return hasResponse(res, {
          status: 'success',
          message: 'Notification token removed successfully',
          fid,
          action,
        });
      } else {
        this.logger.warn(`⚠️ Unhandled action: ${action}`);
        return hasResponse(res, {
          status: 'ignored',
          message: `Action '${action}' was received but not handled`,
          fid,
          action,
        });
      }
    } catch (error) {
      this.logger.error('❌ Error processing Farcaster webhook:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'farcasterWebhook',
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
