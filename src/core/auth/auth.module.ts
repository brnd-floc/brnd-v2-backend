// Dependencies
import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';

// Controllers
import { AuthController } from './auth.controller';

// Services
import { AuthService } from './services';
import { UserService } from '../user/services';

// Models
import {
  Brand,
  User,
  UserBrandVotes,
  UserDailyActions,
  AirdropSnapshot,
  AirdropScore,
} from '../../models';
import { AdminGuard } from 'src/security/guards';
import { AirdropModule } from '../airdrop/airdrop.module';

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
    forwardRef(() => AirdropModule),
  ],
  controllers: [AuthController],
  providers: [AuthService, UserService, AdminGuard],
  exports: [AuthService, AdminGuard],
})
export class AuthModule {}
