// Dependencies
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

// Controllers
import { BrandController } from './brand.controller';

// Services
import { BrandService } from './services';
import { UserService } from '../user/services';
import { BrandSeederService } from './services/brand-seeding.service';
import { AdminService } from '../admin/services/admin.service';
import { IpfsService } from '../../utils/ipfs.service';

// Models
import {
  Brand,
  User,
  UserBrandVotes,
  UserDailyActions,
  Category,
  RewardClaim,
  AirdropScore,
  AirdropSnapshot,
} from '../../models';
import { AuthModule } from '../auth/auth.module';
import { BrandSchedulerService } from './services/brand-scheduler.service';
import { BlockchainModule } from '../blockchain/blockchain.module';
import { NotificationModule } from '../notification/notification.module';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      User,
      Brand,
      UserBrandVotes,
      UserDailyActions,
      Category,
      RewardClaim,
      AirdropScore,
      AirdropSnapshot,
    ]),
    AuthModule,
    BlockchainModule,
    NotificationModule,
  ],
  controllers: [BrandController],
  providers: [
    BrandService,
    UserService,
    BrandSeederService,
    AdminService,
    BrandSchedulerService,
    IpfsService,
  ],
  exports: [BrandService],
})
export class BrandModule {}
