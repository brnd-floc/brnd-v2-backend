// Dependencies
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AuthModule } from '../auth/auth.module';
import { AirdropModule } from '../airdrop/airdrop.module';

// Controllers
import { UserController } from './user.controller';

// Services
import { UserService } from './services';

// Models
import { Brand, User, UserBrandVotes, UserDailyActions, AirdropSnapshot, AirdropScore } from '../../models';
@Module({
  imports: [
    TypeOrmModule.forFeature([User, Brand, UserBrandVotes, UserDailyActions, AirdropSnapshot, AirdropScore]),
    AuthModule,
    AirdropModule,
  ],
  controllers: [UserController],
  providers: [UserService],
  exports: [UserService],
})
export class UserModule {}
