// src/core/notification/notification.module.ts

import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ScheduleModule } from '@nestjs/schedule';

import { NotificationController } from './notification.controller';
import { NeynarNotificationService, NotificationScheduler } from './services';
import { User } from '../../models';

@Module({
  imports: [
    TypeOrmModule.forFeature([User]),
    ScheduleModule.forRoot(),
  ],
  controllers: [NotificationController],
  providers: [NeynarNotificationService, NotificationScheduler],
  exports: [NeynarNotificationService],
})
export class NotificationModule {}
