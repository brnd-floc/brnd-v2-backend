import { IsString, IsNumber, IsNotEmpty } from 'class-validator';

export class ClaimFeesSignatureDto {
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

