import { IsString, IsNumber, IsNotEmpty } from 'class-validator';

export class BuyPodiumSignatureDto {
  @IsString()
  @IsNotEmpty()
  walletAddress: string;

  @IsNumber()
  @IsNotEmpty()
  tokenId: number;

  @IsNumber()
  @IsNotEmpty()
  deadline: number;
}

