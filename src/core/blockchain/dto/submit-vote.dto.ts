import {
  IsString,
  IsNumber,
  IsArray,
  ArrayMinSize,
  ArrayMaxSize,
  IsOptional,
} from 'class-validator';

export class SubmitVoteDto {
  @IsString()
  id: string; // Vote ID format: "0xabc123...-42"

  @IsString()
  voter: string; // Ethereum address

  @IsNumber()
  fid: number; // Farcaster ID

  @IsString()
  day: string; // Day number as string

  @IsArray()
  @ArrayMinSize(3)
  @ArrayMaxSize(3)
  @IsNumber({}, { each: true })
  brandIds: [number, number, number]; // [1st, 2nd, 3rd place]

  @IsString()
  cost: string; // BigInt as string (in wei)

  @IsString()
  blockNumber: string; // Block number as string

  @IsString()
  transactionHash: string; // Transaction hash

  @IsString()
  timestamp: string; // Unix timestamp as string
}
