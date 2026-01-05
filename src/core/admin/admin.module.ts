// src/core/admin/admin.module.ts
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

// Controllers
import { AdminController } from './admin.controller';

// Services
import { AdminService } from './services/admin.service';

// Models
import { Brand, Category, UserBrandVotes } from '../../models';

// Other modules
import { AuthModule } from '../auth/auth.module';
import { BlockchainModule } from '../blockchain/blockchain.module';
import { AirdropModule } from '../airdrop/airdrop.module';
import { EmbedsModule } from '../embeds/embeds.module';
import { NotificationModule } from '../notification/notification.module';
import { BrandModule } from '../brand/brand.module';
import { IpfsService } from '../../utils/ipfs.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([Brand, Category, UserBrandVotes]),
    AuthModule,
    BlockchainModule,
    AirdropModule,
    EmbedsModule,
    NotificationModule,
    BrandModule,
  ],
  controllers: [AdminController],
  providers: [AdminService, IpfsService],
  exports: [AdminService],
})
export class AdminModule {}
