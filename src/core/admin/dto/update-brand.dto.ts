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
}
