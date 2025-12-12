// Dependencies
import { AuthModule } from './auth/auth.module';
import { BrandModule } from './brand/brand.module';
import { UserModule } from './user/user.module';
import { VoteModule } from './vote/vote.module';
import { NotificationModule } from './notification/notification.module';
import { EmbedsModule } from './embeds/embeds.module';
import { AdminModule } from './admin/admin.module';
import { AirdropModule } from './airdrop/airdrop.module';
import { BlockchainModule } from './blockchain/blockchain.module';
import { DailyModule } from './daily/daily.module';

const CoreModules = [
  UserModule,
  BrandModule,
  AuthModule,
  VoteModule,
  NotificationModule,
  EmbedsModule,
  AdminModule,
  AirdropModule,
  BlockchainModule,
  DailyModule,
];

export default CoreModules;
