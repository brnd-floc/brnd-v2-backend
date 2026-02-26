// src/core/notification/notification.module.ts

import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { WebhookApiKeyGuard } from '../../security/guards';

import { NotificationController } from './notification.controller';
import { FarcasterNotificationService } from './services';
import { User, UserBrandVotes } from '../../models';

@Module({
  imports: [TypeOrmModule.forFeature([User, UserBrandVotes])],
  controllers: [NotificationController],
  providers: [FarcasterNotificationService, WebhookApiKeyGuard],
  exports: [FarcasterNotificationService],
})
export class NotificationModule {}
