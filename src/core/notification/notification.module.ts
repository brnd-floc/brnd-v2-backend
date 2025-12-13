// src/core/notification/notification.module.ts

import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ScheduleModule } from '@nestjs/schedule';

import { NotificationController } from './notification.controller';
import { FarcasterNotificationService } from './services';
import { User, UserBrandVotes } from '../../models';

@Module({
  imports: [
    TypeOrmModule.forFeature([User, UserBrandVotes]),
    ScheduleModule.forRoot(),
  ],
  controllers: [NotificationController],
  providers: [FarcasterNotificationService],
  exports: [FarcasterNotificationService],
})
export class NotificationModule {}
