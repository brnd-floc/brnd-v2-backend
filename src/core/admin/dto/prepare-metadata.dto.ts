import {
  IsString,
  IsNumber,
  IsBoolean,
  IsNotEmpty,
  IsOptional,
} from 'class-validator';

// Matches BrandFormData interface from frontend
export class PrepareMetadataDto {
  @IsString()
  @IsNotEmpty()
  name: string;

  @IsString()
  @IsNotEmpty()
  url: string;

  @IsString()
  @IsOptional()
  warpcastUrl?: string;

  @IsString()
  @IsNotEmpty()
  description: string;

  @IsNumber()
  @IsOptional()
  categoryId: number;

  @IsNumber()
  @IsOptional()
  followerCount: number;

  @IsString()
  @IsOptional()
  imageUrl: string;

  @IsString()
  @IsOptional()
  profile: string;

  @IsString()
  @IsOptional()
  channel: string;

  @IsNumber()
  queryType: number;

  @IsString()
  @IsOptional()
  channelOrProfile?: string;

  // Additional fields for on-chain creation (not part of BrandFormData, but needed for response)
  @IsString()
  @IsNotEmpty()
  handle: string; // Brand handle for on-chain creation

  @IsNumber()
  fid: number; // Farcaster ID for on-chain creation

  @IsString()
  @IsNotEmpty()
  walletAddress: string; // Wallet address for on-chain creation (0x format)

  @IsBoolean()
  isEditing: boolean;

  @IsString()
  @IsOptional()
  contractAddress?: string;

  @IsString()
  @IsOptional()
  ticker?: string;

  @IsString()
  @IsOptional()
  tokenContractAddress?: string;

  @IsString()
  @IsOptional()
  tokenTicker?: string;

  @IsString()
  @IsOptional()
  tickerTokenId?: string;

  @IsNumber()
  @IsOptional()
  guardianFid?: number;
}
