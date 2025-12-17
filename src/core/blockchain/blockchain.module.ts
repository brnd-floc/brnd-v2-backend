import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

import { BlockchainController } from './blockchain.controller';
import { BlockchainService } from './services/blockchain.service';
import { PowerLevelService } from './services/power-level.service';
import { SignatureService } from './services/signature.service';
import { RewardService } from './services/reward.service';
import { CastVerificationService } from './services/cast-verification.service';
import { ContractUploadService } from './services/contract-upload.service';
import { IndexerService } from './services/indexer.service';
import { UserService } from '../user/services';
import { BrandService } from '../brand/services';
import { AdminService } from '../admin/services/admin.service';
import { IpfsService } from '../../utils/ipfs.service';
import { AuthModule } from '../auth/auth.module';
import { EmbedsModule } from '../embeds/embeds.module';

import {
  User,
  Brand,
  UserBrandVotes,
  UserDailyActions,
  Category,
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
      Category,
      AirdropSnapshot,
      AirdropScore,
    ]),
    forwardRef(() => AuthModule),
    EmbedsModule,
  ],
  controllers: [BlockchainController],
  providers: [
    BlockchainService,
    PowerLevelService,
    SignatureService,
    RewardService,
    CastVerificationService,
    ContractUploadService,
    IndexerService,
    UserService,
    BrandService,
    AdminService,
    IpfsService,
  ],
  exports: [
    BlockchainService,
    PowerLevelService,
    SignatureService,
    RewardService,
    CastVerificationService,
    ContractUploadService,
    IndexerService,
  ],
})
export class BlockchainModule {}
