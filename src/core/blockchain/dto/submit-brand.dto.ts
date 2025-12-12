import { IsString, IsNumber } from 'class-validator';

export class SubmitBrandDto {
  @IsNumber()
  id: number; // Brand ID

  @IsNumber()
  fid: number; // Farcaster ID

  @IsString()
  walletAddress: string; // Ethereum address

  @IsString()
  handle: string; // Brand handle

  @IsString()
  createdAt: string; // Unix timestamp as string

  @IsString()
  blockNumber: string; // Block number as string

  @IsString()
  transactionHash: string; // Transaction hash

  @IsString()
  timestamp: string; // Unix timestamp as string
}
