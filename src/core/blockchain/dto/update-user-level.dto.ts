import { IsString, IsNumber } from 'class-validator';

export class UpdateUserLevelDto {
  @IsNumber()
  fid: number; // Farcaster ID of the user (primary identifier)

  @IsNumber()
  brndPowerLevel: number; // New BRND power level for the user

  @IsString()
  wallet: string; // Wallet address associated with the level-up (lowercase hex)

  @IsString()
  levelUpId: string; // Unique identifier: `${transactionHash}-${logIndex}`

  @IsString()
  blockNumber: string; // Block number where the level-up occurred (as string)

  @IsString()
  transactionHash: string; // Hash of the transaction

  @IsString()
  timestamp: string; // Block timestamp as string (Unix timestamp)
}
