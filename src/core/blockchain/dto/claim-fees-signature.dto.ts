import { IsString, IsNumber, IsNotEmpty } from 'class-validator';

export class ClaimFeesSignatureDto {
  @IsString()
  @IsNotEmpty()
  walletAddress: string;

  @IsString()
  @IsNotEmpty()
  tokenId: string; // Accept as string to handle large uint256 values

  @IsNumber()
  @IsNotEmpty()
  deadline: number;
}

