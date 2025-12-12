import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { DailyService } from './daily.service';
import { DailyController } from './daily.controller';
import { UserService } from '../user/services';
import { AirdropModule } from '../airdrop/airdrop.module';

import {
  User,
  UserBrandVotes,
  UserDailyActions,
  Brand,
  AirdropScore,
  AirdropSnapshot,
} from '../../models';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      User,
      UserBrandVotes,
      UserDailyActions,
      Brand,
      AirdropScore,
      AirdropSnapshot,
    ]),
    AirdropModule, // Import AirdropModule to access AirdropService
  ],
  controllers: [DailyController],
  providers: [DailyService, UserService],
  exports: [DailyService],
})
export class DailyModule {}
