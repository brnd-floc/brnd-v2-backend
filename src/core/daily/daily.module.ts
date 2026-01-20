import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { DailyService } from './daily.service';
import { DailyController } from './daily.controller';
import { UserService } from '../user/services';
import { AirdropModule } from '../airdrop/airdrop.module';
import { BlockchainModule } from '../blockchain/blockchain.module';

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
    forwardRef(() => BlockchainModule), // Import BlockchainModule to access IndexerSyncService
  ],
  controllers: [DailyController],
  providers: [DailyService, UserService],
  exports: [DailyService],
})
export class DailyModule {}
