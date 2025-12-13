import { Configuration, NeynarAPIClient } from '@neynar/nodejs-sdk';
import { CastResponse } from './types';
import { getConfig } from '../../security/config';
import {
  Cast,
  Channel,
  SearchedUser,
  User,
} from '@neynar/nodejs-sdk/build/api';

export default class NeynarService {
  private client: NeynarAPIClient;

  constructor() {
    const appConfig = getConfig();

    const config = new Configuration({
      apiKey: appConfig.neynar.apiKey,
      baseOptions: {
        headers: {
          'x-neynar-experimental': true,
        },
      },
    });

    this.client = new NeynarAPIClient(config);
  }

  /**
   * Retrieves a cast by its hash from Neynar API.
   * If the cast is not found initially, retries up to 3 times with 1 second delay between attempts.
   *
   * @param castHash - The cast hash (with 0x prefix)
   * @returns Cast data from Neynar
   */
  async getCastByHash(castHash: string): Promise<Cast> {
    const maxRetries = 8;
    let attempts = 0;

    while (attempts < maxRetries) {
      try {
        // Wait 1 second between attempts, except for first attempt
        if (attempts > 0) {
          console.log(
            `⏳ [NeynarService] Waiting 1 second before retry ${attempts}/${maxRetries}...`,
          );
          await new Promise((resolve) =>
            setTimeout(resolve, 1000 + attempts * 1000),
          );
        }

        console.log(
          `🔍 [NeynarService] Attempt ${attempts + 1}/${maxRetries} - Fetching cast: ${castHash}`,
        );
        const response = await this.client.lookupCastByHashOrWarpcastUrl({
          identifier: castHash,
          type: 'hash',
        });
        console.log(
          `✅ [NeynarService] Successfully fetched cast on attempt ${attempts + 1}`,
          response.cast,
        );
        return response.cast;
      } catch (error) {
        attempts++;
        console.log(
          `❌ [NeynarService] Attempt ${attempts}/${maxRetries} failed:`,
          error.message,
        );

        // Check for 404 errors (cast not found)
        const isNotFound =
          error.response?.status === 404 ||
          error.message?.includes('not found') ||
          error.message?.includes('NotFound');

        // If cast not found and we haven't exhausted retries, continue to next attempt
        if (isNotFound && attempts < maxRetries) {
          console.log(
            `🔄 [NeynarService] Cast not found, retrying... (${attempts}/${maxRetries})`,
          );
          continue;
        }

        // If we've exhausted retries or hit a different error, throw
        console.error('❌ [NeynarService] Final error fetching cast:', error);
        throw error;
      }
    }

    throw new Error('Failed to fetch cast after maximum retry attempts');
  }

  getTrendingCastInAChannel = async (
    channel: string,
  ): Promise<CastResponse[]> => {
    const response: CastResponse[] = [];

    try {
      const channelInfo: Channel = (
        await this.client.lookupChannel({ id: channel.slice(1) })
      ).channel;
      const feed = await this.client.fetchFeedByChannelIds({
        channelIds: [channelInfo.id],
        limit: 5,
      });

      for (const cast of feed.casts) {
        let image = '';
        if (cast.embeds.length > 0) {
          const embed = cast.embeds[0];
          if (embed) {
            const metadata = embed['metadata'];
            if (metadata) {
              const contentType = metadata['content_type'];
              if (contentType && contentType.includes('image/'))
                image = embed['url'];
            }
          }
        }

        response.push({
          creator: cast.author.display_name,
          creatorPfp: cast.author.pfp_url,
          creatorPowerBadge: cast.author.power_badge,
          text: cast.text,
          image,
          hash: cast.hash,
          warpcastUrl: `https://warpcast.com/${cast.author.username}/${cast.hash.slice(0, 10)}`,
        });
      }
    } catch (e) {
      console.log(e);
    }

    return response;
  };

  getTrendingCastInAProfile = async (
    profile: string,
  ): Promise<CastResponse[]> => {
    const response: CastResponse[] = [];
    console.log('Getting trending casts for profile:', profile);

    try {
      const searchResult = await this.client.searchUser({
        q: profile.slice(1),
      });
      console.log('Search result:', searchResult);
      const users = searchResult.result.users;
      console.log('Found users:', users);

      let selectedProfile: SearchedUser = undefined;
      for (let i = 0; i < users.length; i++) {
        const user = users[i];
        if (user.username === profile.slice(1)) {
          selectedProfile = user;
          console.log('Found matching profile:', selectedProfile);
        }
      }

      if (selectedProfile !== undefined) {
        console.log('Fetching casts for FID:', selectedProfile.fid);
        const result = await this.client.fetchCastsForUser({
          fid: selectedProfile.fid,
          limit: 5,
        });
        console.log('Fetch result:', result);

        const casts = result.casts;
        console.log('Found casts:', casts);

        for (const cast of casts) {
          console.log('Processing cast:', cast);
          const author = cast.author as User;
          let image = '';

          if (cast.embeds.length > 0) {
            console.log('Cast has embeds:', cast.embeds);
            const embed = cast.embeds[0];
            if (embed) {
              const metadata = embed['metadata'];
              if (metadata) {
                const contentType = metadata['content_type'];
                console.log('Embed content type:', contentType);
                if (contentType && contentType.includes('image/')) {
                  image = embed['url'];
                  console.log('Found image URL:', image);
                }
              }
            }
          }

          const castResponse = {
            creator: author.display_name,
            creatorPfp: author.pfp_url,
            creatorPowerBadge: author.power_badge,
            text: cast.text,
            image,
            warpcastUrl: `https://farcaster.xyz/${author.username}/${cast.hash.slice(0, 10)}`,
            hash: cast.hash,
          };
          console.log('Adding cast response:', castResponse);
          response.push(castResponse);
        }
      } else {
        console.log('No matching profile found');
      }
    } catch (e) {
      console.log('Error in getTrendingCastInAProfile:', e);
    }

    console.log('Returning response:', response);
    return response;
  };

  getUserByFid = async (fid: number): Promise<User> => {
    const response = await this.client.fetchBulkUsers({ fids: [fid] });
    return response.users[0];
  };

  /**
   * Fetches follower count for a channel
   */
  getChannelFollowerCount = async (channelId: string): Promise<number> => {
    try {
      const channelInfo = await this.client.lookupChannel({ id: channelId });
      return channelInfo.channel.follower_count || 0;
    } catch (error) {
      console.error('Error fetching channel follower count:', error);
      throw error;
    }
  };

  /**
   * Fetches follower count for a profile by username
   */
  getProfileFollowerCount = async (username: string): Promise<number> => {
    try {
      const searchResult = await this.client.searchUser({ q: username });

      const matchingUser = searchResult.result.users.find(
        (user) => user.username === username,
      );

      if (matchingUser) {
        return matchingUser.follower_count || 0;
      }

      return 0;
    } catch (error) {
      console.error('Error fetching profile follower count:', error);
      throw error;
    }
  };
}
