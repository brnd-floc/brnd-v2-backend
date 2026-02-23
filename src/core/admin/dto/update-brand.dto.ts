export class UpdateBrandDto {
  name?: string;
  url?: string;
  warpcastUrl?: string;
  description?: string;
  categoryId?: number;
  followerCount?: number;
  imageUrl?: string;
  profile?: string;
  channel?: string;
  queryType?: number;
  channelOrProfile?: string; // From frontend form
  contractAddress?: string;
  ticker?: string;
  tickerTokenId?: string;
  guardianFid?: number; // Canonical guardian fid (legacy mapped to founderFid)
}
