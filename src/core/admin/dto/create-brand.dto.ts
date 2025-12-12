export class CreateBrandDto {
  name: string;
  url: string;
  warpcastUrl?: string; // Optional, will fallback to url
  description: string;
  categoryId?: number; // Optional, will create "General" category if not provided
  followerCount?: number; // Optional, will try to fetch from Neynar
  imageUrl?: string; // Optional
  profile?: string; // Optional
  channel?: string; // Optional
  queryType: number; // 0: Channel, 1: Profile
  channelOrProfile?: string; // From frontend form
}
