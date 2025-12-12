import { Module, forwardRef } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AirdropController } from './airdrop.controller';
import { AirdropService } from './services/airdrop.service';
import { AirdropContractService } from './services/airdrop-contract.service';
import { AirdropScore, AirdropSnapshot, AirdropLeaf, User } from '../../models';
import { AuthModule } from '../auth/auth.module';
import { BlockchainModule } from '../blockchain/blockchain.module';

@Module({
  imports: [
    TypeOrmModule.forFeature([AirdropScore, AirdropSnapshot, AirdropLeaf, User]),
    forwardRef(() => AuthModule),
    forwardRef(() => BlockchainModule), // Import to access SignatureService
  ],
  controllers: [AirdropController],
  providers: [AirdropService, AirdropContractService],
  exports: [AirdropService, AirdropContractService],
})
export class AirdropModule {}
