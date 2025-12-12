import { IsString, IsNumber } from 'class-validator';

export class SubmitRewardClaimDto {
  @IsString()
  id: string; // Unique claim ID: `${transactionHash}-${logIndex}`

  @IsString()
  recipient: string; // Wallet address of the reward recipient (lowercase)

  @IsNumber()
  fid: number; // Farcaster ID of the user claiming the reward

  @IsString()
  amount: string; // Reward amount as a string (bigint converted to string)

  @IsString()
  day: string; // Day number as a string (bigint converted to string)

  @IsString()
  castHash: string; // Hash of the cast associated with the reward claim

  @IsString()
  caller: string; // Wallet address of the caller (lowercase)

  @IsString()
  blockNumber: string; // Block number where the claim occurred (bigint converted to string)

  @IsString()
  transactionHash: string; // Hash of the transaction

  @IsString()
  timestamp: string; // Block timestamp as a string (bigint converted to string)
}
