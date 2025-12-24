// Matches BrandFormData interface from frontend
export class PrepareMetadataDto {
  name: string;
  url: string;
  warpcastUrl?: string;
  description: string;
  categoryId: number;
  followerCount: number;
  imageUrl: string;
  profile: string;
  channel: string;
  queryType: number;
  channelOrProfile?: string;
  // Additional fields for on-chain creation (not part of BrandFormData, but needed for response)
  handle: string; // Brand handle for on-chain creation
  fid: number; // Farcaster ID for on-chain creation
  walletAddress: string; // Wallet address for on-chain creation (0x format)
  isEditing: boolean;
}
