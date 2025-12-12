// src/core/embeds/embeds.module.ts

import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { EmbedsController } from './embeds.controller';
import { EmbedsService } from './services';
import { UserBrandVotes, User, Brand } from '../../models';

@Module({
  imports: [TypeOrmModule.forFeature([UserBrandVotes, User, Brand])],
  controllers: [EmbedsController],
  providers: [EmbedsService],
  exports: [EmbedsService],
})
export class EmbedsModule {}
