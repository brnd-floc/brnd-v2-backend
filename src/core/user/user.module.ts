// Dependencies
import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthModule } from '../auth/auth.module';
import { AirdropModule } from '../airdrop/airdrop.module';
import { BlockchainModule } from '../blockchain/blockchain.module';

// Controllers
import { UserController } from './user.controller';

// Services
import { UserService } from './services';

// Models
import {
  Brand,
  User,
  UserBrandVotes,
  UserDailyActions,
  AirdropSnapshot,
  AirdropScore,
} from '../../models';
@Module({
  imports: [
    TypeOrmModule.forFeature([
      User,
      Brand,
      UserBrandVotes,
      UserDailyActions,
      AirdropSnapshot,
      AirdropScore,
    ]),
    AuthModule,
    AirdropModule,
    forwardRef(() => BlockchainModule), // Import for IndexerSyncService
  ],
  controllers: [UserController],
  providers: [UserService],
  exports: [UserService],
})
export class UserModule {}
