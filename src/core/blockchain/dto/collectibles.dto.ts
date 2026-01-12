// CollectibleMintedDto
import {
  IsNumber,
  IsString,
  IsArray,
  ArrayMinSize,
  ArrayMaxSize,
} from 'class-validator';

export class CollectibleMintedDto {
  @IsNumber()
  tokenId: number;

  @IsArray()
  @ArrayMinSize(3)
  @ArrayMaxSize(3)
  @IsNumber({}, { each: true })
  brandIds: [number, number, number];

  @IsNumber()
  ownerFid: number;

  @IsString()
  ownerWallet: string;

  @IsString()
  price: string;

  @IsString()
  txHash: string;
}

export class CollectibleBoughtDto {
  @IsNumber()
  tokenId: number;

  @IsNumber()
  newOwnerFid: number;

  @IsString()
  newOwnerWallet: string;

  @IsString()
  price: string;

  @IsNumber()
  claimCount: number;
}
