import { getConfig } from '../../security/config';

export interface NeynarUser {
  fid: number;
  username: string;
  display_name: string;
  pfp_url: string;
  follower_count: number;
  following_count: number;
  power_badge: boolean;
}

export interface NeynarFollowingResponse {
  users: NeynarUser[];
}

export interface NeynarUserResponse {
  user: NeynarUser;
}

export interface NeynarUserInteractions {
  fid: number;
  following: boolean;
  followed_by: boolean;
  blocking: boolean;
  blocked_by: boolean;
}

export interface NeynarUserInteractionsResponse {
  interactions: NeynarUserInteractions[];
}

export class NeynarAPI {
  private readonly apiKey: string;
  private readonly baseURL = 'https://api.neynar.com/v2';

  constructor() {
    this.apiKey = getConfig().neynar.apiKey.replace(/&$/, ''); // Remove trailing &
  }

  private async makeRequest<T>(
    endpoint: string,
    params?: Record<string, any>,
  ): Promise<T> {
    const url = new URL(`${this.baseURL}${endpoint}`);

    if (params) {
      Object.keys(params).forEach((key) => {
        if (params[key] !== undefined && params[key] !== null) {
          url.searchParams.append(key, params[key].toString());
        }
      });
    }

    console.log('THE NEYNAR API KEY', this.apiKey);

    const response = await fetch(url.toString(), {
      headers: {
        accept: 'application/json',
        'x-api-key': this.apiKey,
      },
    });

    if (!response.ok) {
      throw new Error(
        `Neynar API error: ${response.status} ${response.statusText}`,
      );
    }

    return response.json();
  }

  async getUserByFid(fid: number): Promise<NeynarUser> {
    const response = await this.makeRequest<NeynarUserResponse>(
      `/farcaster/user/bulk`,
      {
        fids: fid.toString(),
      },
    );
    return response.user;
  }

  async getUserFollowing(
    fid: number,
    limit: number = 100,
  ): Promise<NeynarUser[]> {
    const response = await this.makeRequest<NeynarFollowingResponse>(
      `/farcaster/following`,
      {
        fid: fid.toString(),
        limit: limit.toString(),
      },
    );
    return response.users;
  }

  async getUserFollowers(
    fid: number,
    limit: number = 100,
  ): Promise<NeynarUser[]> {
    const response = await this.makeRequest<NeynarFollowingResponse>(
      `/farcaster/followers`,
      {
        fid: fid.toString(),
        limit: limit.toString(),
      },
    );
    return response.users;
  }

  async searchUser(query: string): Promise<NeynarUser[]> {
    const response = await this.makeRequest<{
      result: { users: NeynarUser[] };
    }>(`/farcaster/user/search`, {
      q: query,
    });
    return response.result.users;
  }

  async getUserInteractions(
    fid: number,
    targetFids: number[],
  ): Promise<NeynarUserInteractions[]> {
    const response = await this.makeRequest<NeynarUserInteractionsResponse>(
      `/farcaster/user/interactions`,
      {
        fid: fid.toString(),
        target_fids: targetFids.join(','),
      },
    );
    return response.interactions;
  }
}
