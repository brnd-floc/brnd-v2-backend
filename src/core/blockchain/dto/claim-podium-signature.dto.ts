import {
  IsString,
  IsNumber,
  IsArray,
  ArrayMinSize,
  ArrayMaxSize,
  IsNotEmpty,
} from 'class-validator';

export class ClaimPodiumSignatureDto {
  @IsString()
  @IsNotEmpty()
  walletAddress: string;

  @IsArray()
  @ArrayMinSize(3)
  @ArrayMaxSize(3)
  @IsNumber({}, { each: true })
  brandIds: [number, number, number];

  @IsNumber()
  @IsNotEmpty()
  deadline: number;
}

