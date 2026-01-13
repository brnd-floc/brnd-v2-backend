// src/app.module.ts - Updated for Production with SSL Support
// Dependencies
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ScheduleModule } from '@nestjs/schedule';
// Core
import CoreModules from './core';
// Security
import { getConfig } from './security/config';
// Models
import {
  User,
  Category,
  Brand,
  Tag,
  UserBrandVotes,
  BrandTags,
  UserDailyActions,
  AirdropScore,
  AirdropSnapshot,
  AirdropLeaf,
  RewardClaim,
  CollectibleActivity,
} from './models';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    ...CoreModules,
    TypeOrmModule.forRoot({
      type: 'mysql',
      host: getConfig().db.host,
      port: getConfig().db.port,
      username: getConfig().db.username,
      password: getConfig().db.password,
      database: getConfig().db.name,
      entities: [
        User,
        Category,
        Brand,
        Tag,
        UserBrandVotes,
        BrandTags,
        UserDailyActions,
        AirdropScore,
        AirdropSnapshot,
        AirdropLeaf,
        RewardClaim,
        CollectibleActivity,
      ],
      // Important: Set synchronize to false in production for safety
      synchronize: false, // Enabled to automatically sync schema changes
      //synchronize: !getConfig().isProduction,
      logging: false,
      timezone: 'Z',
      // SSL configuration for DigitalOcean managed database
      ssl: getConfig().db.requireSSL
        ? {
            rejectUnauthorized: false, // Required for DigitalOcean managed databases
          }
        : false,
      extra: {
        // Keep insecureAuth for development, remove in production
        ...(getConfig().isProduction ? {} : { insecureAuth: true }),
        // Connection pool settings for production
        connectionLimit: 20, // Increased from 10 to handle concurrent operations
        acquireTimeout: 30000, // Reduced from 60000
        timeout: 30000, // Reduced from 60000
      },
    }),
  ],
})
export class AppModule {}
