// src/core/embeds/embeds.module.ts

import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AdminGuard, DebugEndpointGuard } from '../../security/guards';
import { AuthModule } from '../auth/auth.module';
import { EmbedsController } from './embeds.controller';
import { EmbedsService, PodiumService } from './services';
import { UserBrandVotes, User, Brand } from '../../models';

@Module({
  imports: [TypeOrmModule.forFeature([UserBrandVotes, User, Brand]), AuthModule],
  controllers: [EmbedsController],
  providers: [EmbedsService, PodiumService, AdminGuard, DebugEndpointGuard],
  exports: [EmbedsService, PodiumService],
})
export class EmbedsModule {}
