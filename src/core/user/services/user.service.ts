// Dependencies
import { Injectable, Inject, Optional } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Between, Repository, In, MoreThan } from 'typeorm';

// Models
import {
  User,
  UserBrandVotes,
  UserRoleEnum,
  UserDailyActions,
  Brand,
  UserBrandRanking,
  AirdropSnapshot,
  AirdropScore,
} from '../../../models';
import { logger } from 'src/main';
import { AirdropContractService } from '../../airdrop/services/airdrop-contract.service';
import { getConfig } from '../../../security/config';

/**
 * Interface for leaderboard response with user position info
 */
export interface LeaderboardResponse {
  users: User[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
    hasNextPage: boolean;
    hasPrevPage: boolean;
  };
  currentUser?: {
    position: number;
    points: number;
    user: Pick<User, 'id' | 'fid' | 'username' | 'photoUrl'>;
  };
}

@Injectable()
export class UserService {
  /**
   * Cache for leaderboard data
   */
  private leaderboardCache: {
    users: User[];
    lastUpdated: Date;
    total: number;
  } | null = null;

  /**
   * Cache TTL in milliseconds (15 minutes)
   */
  private readonly CACHE_TTL = 15 * 60 * 1000;

  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,

    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,

    @InjectRepository(UserDailyActions)
    private readonly userDailyActionsRepository: Repository<UserDailyActions>,

    @InjectRepository(Brand)
    private readonly brandRespository: Repository<Brand>,

    @InjectRepository(AirdropSnapshot)
    private readonly airdropSnapshotRepository: Repository<AirdropSnapshot>,

    @InjectRepository(AirdropScore)
    private readonly airdropScoreRepository: Repository<AirdropScore>,

    @Optional()
    private readonly airdropContractService?: AirdropContractService,
  ) {}

  /**
   * Retrieves a user by their ID with optional selected fields and relations.
   *
   * @param {User['id']} id - The ID of the user to retrieve.
   * @param {(keyof User)[]} [select=[]] - Optional array of fields to select.
   * @param {(keyof User)[]} [relations=[]] - Optional array of relations to include.
   * @returns {Promise<User | undefined>} The user entity or undefined if not found.
   */
  async getById(
    id: User['id'],
    select: (keyof User)[] = [],
    relations: (keyof User)[] = [],
  ): Promise<User | undefined> {
    return this.userRepository.findOne({
      ...(select.length > 0 && {
        select,
      }),
      where: {
        id,
      },
      ...(relations.length > 0 && {
        relations,
      }),
    });
  }

  /**
   * Checks if a user has voted on a specific date (for voting system).
   * This is a simpler version of getUserVotes that works with date strings.
   *
   * @param {User['id']} userId - The user's database ID
   * @param {string} dateString - Date in YYYY-MM-DD format
   * @returns {Promise<UserBrandVotes | undefined>} Vote record or undefined
   */
  async hasVotedOnDate(
    userId: User['id'],
    dateString: string,
  ): Promise<UserBrandVotes | undefined> {
    return this.userBrandVotesRepository.findOne({
      where: {
        user: { id: userId },
        date: new Date(dateString), // Direct string comparison
      },
    });
  }

  /**
   * Retrieves a user by their Farcaster ID with optional selected fields and relations.
   *
   * @param {User['fid']} fid - The Farcaster ID of the user to retrieve.
   * @param {(keyof User)[]} [select=[]] - Optional array of fields to select.
   * @param {(keyof User)[]} [relations=[]] - Optional array of relations to include.
   * @returns {Promise<User | undefined>} The user entity or undefined if not found.
   */
  async getByFid(
    fid: User['fid'],
    select: (keyof User)[] = [],
    relations: (keyof User)[] = [],
  ): Promise<User | undefined> {
    return this.userRepository.findOne({
      ...(select.length > 0 && {
        select,
      }),
      where: {
        fid,
      },
      ...(relations.length > 0 && {
        relations,
      }),
    });
  }

  /**
   * Upserts a user based on the provided Firebase ID. This method checks if a user with the given Firebase ID exists. If the user exists, it updates the user with the provided data; otherwise, it creates a new user with the given data and assigns a default role of USER.
   *
   * @param {User['fid']} fid - The Firebase ID of the user to upsert.
   * @param {Partial<User>} data - An object containing the fields to update for an existing user or to set for a new user.
   * @returns {Promise<{isCreated: boolean; user: User}>} An object containing a boolean flag indicating if a new user was created and the upserted user entity.
   */
  async upsert(
    fid: User['fid'],
    data: Partial<User>,
  ): Promise<{ isCreated: boolean; user: User }> {
    let isCreated: boolean = false;
    let user = await this.userRepository.findOne({
      where: {
        fid,
      },
    });

    if (user) {
      Object.assign(user, data);
    } else {
      isCreated = true;
      user = this.userRepository.create({
        fid,
        ...data,
        role: UserRoleEnum.USER,
      });
    }

    await this.userRepository.save(user);

    return {
      isCreated,
      user,
    };
  }

  /**
   * Creates a new user with the provided Firebase ID, username, and photo URL.
   *
   * @param {User['fid']} fid - The Firebase ID of the user.
   * @param {User['username']} username - The username of the user.
   * @param {User['photoUrl']} photoUrl - The photo URL of the user.
   * @returns {Promise<User>} The newly created user entity.
   */
  async create(
    fid: User['fid'],
    username: User['username'],
    photoUrl: User['photoUrl'],
  ): Promise<User> {
    const newUser = this.userRepository.create({
      fid,
      username,
      photoUrl,
    });

    await this.userRepository.save(newUser);
    return newUser;
  }

  /**
   * Updates a user's data based on the provided user ID.
   *
   * @param {User['id']} id - The ID of the user to update.
   * @param {Partial<User>} data - An object containing the fields to update.
   * @returns {Promise<User>} The updated user entity.
   * @throws {Error} If the user with the specified ID is not found.
   */
  async update(id: User['id'], data: Partial<User>): Promise<User> {
    const user = await this.userRepository.findOne({
      where: {
        id,
      },
    });

    if (!user) {
      throw new Error(`User with ID ${id} not found.`);
    }

    Object.assign(user, data);
    await this.userRepository.save(user);

    return user;
  }

  /**
   * Adds points to a user's account.
   * Also invalidates leaderboard cache for real-time updates.
   *
   * @param {User['id']} userId - The ID of the user to add points to.
   * @param {number} points - The number of points to add.
   * @throws {Error} If the user with the specified ID is not found.
   */
  async addPoints(userId: User['id'], points: number) {
    const user = await this.getById(userId);

    if (!user) {
      throw new Error(`User with ID ${userId} not found.`);
    }

    user.points += points;
    const updatedUser = await this.userRepository.save(user);

    // Invalidate leaderboard cache for real-time updates
    this.invalidateLeaderboardCache();

    console.log(
      `💰 [UserService] Added ${points} points to user ${userId}, total: ${user.points}`,
    );

    return updatedUser;
  }

  /**
   * Removes points from a user's account.
   *
   * @param {User['id']} userId - The ID of the user to remove points from.
   * @param {number} points - The number of points to remove.
   * @throws {Error} If the user with the specified ID is not found.
   */
  async removePoints(userId: User['id'], points: number) {
    const user = await this.getById(userId);

    if (!user) {
      throw new Error(`User with ID ${userId} not found.`);
    }

    user.points -= points;
    await this.userRepository.save(user);
  }

  async addPointsForShareFrame(userId: User['id']): Promise<boolean> {
    let result = false;

    // Add 3 points for sharing a frame only the first time
    const user = await this.getById(userId);

    if (!user) {
      throw new Error(`User with ID ${userId} not found.`);
    }

    let userDailyActions = await this.userDailyActionsRepository.findOne({
      where: {
        user: { id: userId },
      },
      relations: ['user'],
    });

    if (!userDailyActions) {
      user.points += 3;
      await this.userRepository.save(user);

      userDailyActions = this.userDailyActionsRepository.create({
        user: user,
        shareFirstTime: true,
      });
      await this.userDailyActionsRepository.save(userDailyActions);

      result = true;
    } else if (userDailyActions.shareFirstTime === false) {
      user.points += 3;
      await this.userRepository.save(user);

      userDailyActions.shareFirstTime = true;
      await this.userDailyActionsRepository.save(userDailyActions);

      result = true;
    }

    // Invalidate leaderboard cache when points are added
    if (result) {
      this.invalidateLeaderboardCache();
    }

    return result;
  }

  /**
   * Deletes a user by their ID.
   *
   * @param {User['id']} id - The ID of the user to delete.
   * @returns {Promise<boolean>} Returns true if the user was successfully deleted.
   * @throws {Error} If the user with the specified ID is not found.
   */
  async delete(id: User['id']): Promise<boolean> {
    const user = await this.userRepository.findOne({
      where: {
        id,
      },
    });

    if (!user) {
      throw new Error(`User with ID ${id} not found.`);
    }

    await this.userRepository.remove(user);

    return true;
  }

  /**
   * Retrieves the votes of a user for a specific day.
   *
   * @param {User['id']} id - The ID of the user whose votes are to be retrieved.
   * @param {number} unixDate - The Unix timestamp representing the day for which votes are to be retrieved.
   * @returns {Promise<UserBrandVotes>} A promise that resolves to an object of the user's votes for the specified day.
   */
  async getUserVotes(
    id: User['id'],
    unixDate: number,
  ): Promise<UserBrandVotes> {
    const date = new Date(unixDate * 1000);
    const startDate = new Date(date.setHours(0, 0, 0, 0));
    const endDate = new Date(date.setHours(23, 59, 59, 999));

    const user = await this.userRepository.findOne({
      select: ['userBrandVotes'],
      where: {
        id,
        userBrandVotes: {
          date: Between(startDate, endDate),
        },
      },
      relations: [
        'userBrandVotes',
        'userBrandVotes.brand1',
        'userBrandVotes.brand2',
        'userBrandVotes.brand3',
      ],
    });

    const userBrandVotes = user ? user.userBrandVotes[0] : undefined;

    return userBrandVotes;
  }

  /**
   * Retrieves the vote history of a user, grouped by day.
   *
   * @param {User['id']} userId - The ID of the user whose vote history is to be retrieved.
   * @param {number} [pageId=1] - The page number for pagination.
   * @param {number} [limit=15] - The number of records to retrieve per page.
   * @returns {Promise<{ count: number; data: Record<string, UserBrandVotes[]> }>} A promise that resolves to an object containing the total count of votes and a record where keys are dates and values are arrays of votes for that day.
   */
  async getVotesHistory(
    userId: User['id'],
    pageId: number = 1,
    limit: number = 15,
  ): Promise<{ count: number; data: Record<string, UserBrandVotes[]> }> {
    const [votes, count] = await this.userBrandVotesRepository.findAndCount({
      where: { user: { id: userId } },
      relations: ['brand1', 'brand2', 'brand3'],
      order: { date: 'DESC' },
      skip: (pageId - 1) * limit,
      take: limit,
    });

    // Return empty response with correct structure if no votes found
    if (count === 0) {
      return {
        count: 0,
        data: {},
      };
    }

    const groupedVotes = votes.reduce((acc, vote) => {
      const dateKey = vote.date.toISOString().split('T')[0]; // Group by date (YYYY-MM-DD)
      if (!acc[dateKey]) {
        acc[dateKey] = {};
      }
      acc[dateKey] = {
        transactionHash: vote.transactionHash,
        date: vote.date,
        brand1: {
          id: vote.brand1.id,
          name: vote.brand1.name,
          imageUrl: vote.brand1.imageUrl,
          score: vote.brand1.score,
          stateScore: vote.brand1.stateScore,
          ranking: vote.brand1.ranking,
        },
        brand2: {
          id: vote.brand2.id,
          name: vote.brand2.name,
          imageUrl: vote.brand2.imageUrl,
          score: vote.brand2.score,
          stateScore: vote.brand2.stateScore,
          ranking: vote.brand2.ranking,
        },
        brand3: {
          id: vote.brand3.id,
          name: vote.brand3.name,
          imageUrl: vote.brand3.imageUrl,
          score: vote.brand3.score,
          stateScore: vote.brand3.stateScore,
          ranking: vote.brand3.ranking,
        },
      };
      return acc;
    }, {});

    return {
      count,
      data: groupedVotes,
    };
  }

  /**
   * Gets a user's personal brand rankings based on all their votes.
   * Aggregates points: 60 for 1st place, 30 for 2nd place, 10 for 3rd place.
   * Returns brands sorted by total points descending.
   *
   * @param {number} userId - The user's database ID
   * @returns {Promise<UserBrandRanking[]>} Array of brands with user's total points
   */
  async getUserBrands(userId: number): Promise<UserBrandRanking[]> {
    try {
      console.log(
        `📊 [UserService] Calculating personal brand rankings for user ID: ${userId}`,
      );

      // Raw SQL query to aggregate user's votes and calculate points
      const query = `
      SELECT 
        brand_id,
        brand_name,
        brand_image_url,
        brand_category_id,
        category_name,
        SUM(points) as total_points,
        COUNT(*) as vote_count,
        MAX(vote_date) as last_voted
      FROM (
        -- 1st place votes (60 points)
        SELECT 
          b.id as brand_id,
          b.name as brand_name,
          b.imageUrl as brand_image_url,
          b.categoryId as brand_category_id,
          c.name as category_name,
          60 as points,
          ubv.date as vote_date
        FROM user_brand_votes ubv
        JOIN brands b ON b.id = ubv.brand1Id
        LEFT JOIN categories c ON c.id = b.categoryId
        WHERE ubv.userId = ?
        
        UNION ALL
        
        -- 2nd place votes (30 points)
        SELECT 
          b.id as brand_id,
          b.name as brand_name,
          b.imageUrl as brand_image_url,
          b.categoryId as brand_category_id,
          c.name as category_name,
          30 as points,
          ubv.date as vote_date
        FROM user_brand_votes ubv
        JOIN brands b ON b.id = ubv.brand2Id
        LEFT JOIN categories c ON c.id = b.categoryId
        WHERE ubv.userId = ?
        
        UNION ALL
        
        -- 3rd place votes (10 points)
        SELECT 
          b.id as brand_id,
          b.name as brand_name,
          b.imageUrl as brand_image_url,
          b.categoryId as brand_category_id,
          c.name as category_name,
          10 as points,
          ubv.date as vote_date
        FROM user_brand_votes ubv
        JOIN brands b ON b.id = ubv.brand3Id
        LEFT JOIN categories c ON c.id = b.categoryId
        WHERE ubv.userId = ?
      ) vote_aggregates
      GROUP BY brand_id, brand_name, brand_image_url, brand_category_id, category_name
      ORDER BY total_points DESC, vote_count DESC, last_voted DESC
    `;

      const results = await this.userRepository.query(query, [
        userId,
        userId,
        userId,
      ]);

      console.log(`📊 [UserService] Found ${results.length} brands for user`);

      // Transform results into the expected format
      const userBrands: UserBrandRanking[] = results.map((result, index) => ({
        brand: {
          id: result.brand_id,
          name: result.brand_name,
          imageUrl: result.brand_image_url,
          category: result.category_name
            ? {
                id: result.brand_category_id,
                name: result.category_name,
              }
            : null,
          // We don't need the scoring fields for personal rankings
          score: 0,
          stateScore: 0,
          scoreDay: 0,
          stateScoreDay: 0,
          ranking: '',
          scoreWeek: 0,
          stateScoreWeek: 0,
          rankingWeek: 0,
          scoreMonth: 0,
          stateScoreMonth: 0,
          rankingMonth: 0,
        } as Brand,
        points: parseInt(result.total_points),
        voteCount: parseInt(result.vote_count),
        lastVoted: result.last_voted,
        position: index + 1, // User's personal ranking position
      }));

      return userBrands;
    } catch (error) {
      console.error(
        '❌ [UserService] Error calculating user brand rankings:',
        error,
      );
      throw new Error('Failed to calculate personal brand rankings');
    }
  }

  /**
   * Gets the leaderboard with pagination and current user position.
   * Uses 15-minute cache for performance.
   *
   * @param {number} page - Page number for pagination
   * @param {number} limit - Number of users per page
   * @param {number} currentUserFid - FID of the current user (to show their position)
   * @returns {Promise<LeaderboardResponse>} Leaderboard data with pagination
   */
  async getLeaderboard(
    page: number = 1,
    limit: number = 50,
    currentUserFid?: number,
  ): Promise<LeaderboardResponse> {
    console.log(
      `🏆 [UserService] Getting leaderboard - page: ${page}, limit: ${limit}`,
    );

    // Check if we need to refresh cache
    await this.refreshLeaderboardCacheIfNeeded();

    const { users: allUsers, total } = this.leaderboardCache!;

    // Calculate pagination
    const skip = (page - 1) * limit;
    const paginatedUsers = allUsers.slice(skip, skip + limit);
    const totalPages = Math.ceil(total / limit);

    // Build pagination info
    const pagination = {
      page,
      limit,
      total,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
    };

    // Find current user position if FID provided
    let currentUser: LeaderboardResponse['currentUser'];
    if (currentUserFid) {
      const userIndex = allUsers.findIndex(
        (user) => user.fid === currentUserFid,
      );
      if (userIndex !== -1) {
        const user = allUsers[userIndex];
        currentUser = {
          position: userIndex + 1,
          points: user.points,
          user: {
            id: user.id,
            fid: user.fid,
            username: user.username,
            photoUrl: user.photoUrl,
          },
        };
      }
    }

    console.log(
      `🏆 [UserService] Returning ${paginatedUsers.length} users, total: ${total}`,
    );

    return {
      users: paginatedUsers,
      pagination,
      currentUser,
    };
  }

  async getTodaysVoteWithBrands(userId: number): Promise<any | null> {
    try {
      const currentUTCDate = new Date().toISOString().split('T')[0];

      const vote = await this.userBrandVotesRepository.findOne({
        where: {
          user: { id: userId },
          date: new Date(currentUTCDate),
        },
        relations: ['brand1', 'brand2', 'brand3'],
        select: {
          id: true,
          date: true,
          brand1: {
            id: true,
            name: true,
            imageUrl: true,
          },
          brand2: {
            id: true,
            name: true,
            imageUrl: true,
          },
          brand3: {
            id: true,
            name: true,
            imageUrl: true,
          },
        },
      });

      return vote;
    } catch (error) {
      logger.error("Error getting today's vote with brands:", error);
      return null;
    }
  }

  /**
   * Gets the current vote status for today including vote, share, and claim status.
   *
   * @param userFid - The user's Farcaster ID
   * @returns Today's vote status or null if user hasn't voted today
   */
  async getTodaysVoteStatus(userFid: number): Promise<{
    hasVoted: boolean;
    hasShared: boolean;
    hasClaimed: boolean;
    voteId: string | null;
    castHash: string | null;
    transactionHash: string | null;
    day: number;
  } | null> {
    try {
      // Calculate today's day number (unix timestamp / 86400)
      const now = Math.floor(Date.now() / 1000);
      const day = Math.floor(now / 86400);

      // Look up today's vote for this user
      const vote = await this.userBrandVotesRepository.findOne({
        where: {
          user: { fid: userFid },
          day: day,
        },
        relations: ['user'],
      });

      if (!vote) {
        return null; // No vote today
      }

      // Determine status
      const hasVoted = true;
      const hasShared = !!(vote.castHash && vote.shared);
      const hasClaimed = !!vote.claimedAt;

      return {
        hasVoted,
        hasShared,
        hasClaimed,
        voteId: vote.transactionHash,
        castHash: vote.castHash,
        transactionHash: vote.transactionHash,
        day: vote.day,
      };
    } catch (error) {
      console.error(
        "❌ [UserService] Error getting today's vote status:",
        error,
      );
      throw new Error('Failed to retrieve vote status');
    }
  }

  /**
   * Refreshes the leaderboard cache if it's stale or doesn't exist.
   *
   * @private
   */
  private async refreshLeaderboardCacheIfNeeded(): Promise<void> {
    const now = new Date();
    const shouldRefresh =
      !this.leaderboardCache ||
      now.getTime() - this.leaderboardCache.lastUpdated.getTime() >
        this.CACHE_TTL;

    if (shouldRefresh) {
      console.log('🔄 [UserService] Refreshing leaderboard cache...');
      await this.refreshLeaderboardCache();
    } else {
      console.log('✅ [UserService] Using cached leaderboard data');
    }
  }

  /**
   * Refreshes the leaderboard cache by querying all users sorted by points.
   *
   * @private
   */
  private async refreshLeaderboardCache(): Promise<void> {
    try {
      const users = await this.userRepository.find({
        select: ['id', 'fid', 'username', 'photoUrl', 'points', 'createdAt'],
        order: {
          points: 'DESC',
          createdAt: 'ASC', // Ties broken by earliest registration
        },
      });

      this.leaderboardCache = {
        users,
        total: users.length,
        lastUpdated: new Date(),
      };

      console.log(
        `✅ [UserService] Leaderboard cache refreshed with ${users.length} users`,
      );
    } catch (error) {
      console.error(
        '❌ [UserService] Failed to refresh leaderboard cache:',
        error,
      );
      // Keep old cache if refresh fails
    }
  }

  /**
   * Invalidates the leaderboard cache.
   * Call this when users gain points to get fresher data.
   *
   * @public
   */
  public invalidateLeaderboardCache(): void {
    console.log('🗑️ [UserService] Invalidating leaderboard cache');
    this.leaderboardCache = null;
  }

  /**
   * Calculates daily voting streak for a user based on consecutive voting days
   */
  async calculateDailyStreak(userId: number): Promise<number> {
    try {
      const votes = await this.userBrandVotesRepository.find({
        where: { user: { id: userId } },
        order: { date: 'DESC' },
        select: ['date'],
      });

      if (votes.length === 0) return 0;

      let streak = 0;
      const now = new Date();
      const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());

      // Check if user voted today or yesterday (streak can continue)
      const mostRecentVote = new Date(votes[0].date);
      const mostRecentVoteDate = new Date(
        mostRecentVote.getFullYear(),
        mostRecentVote.getMonth(),
        mostRecentVote.getDate(),
      );
      const daysDiff = Math.floor(
        (today.getTime() - mostRecentVoteDate.getTime()) /
          (1000 * 60 * 60 * 24),
      );

      if (daysDiff > 1) {
        return 0; // Streak broken
      }

      // Count consecutive days
      const expectedDate = new Date(mostRecentVoteDate);
      for (const vote of votes) {
        const voteDate = new Date(vote.date);
        const voteDateOnly = new Date(
          voteDate.getFullYear(),
          voteDate.getMonth(),
          voteDate.getDate(),
        );

        if (voteDateOnly.getTime() === expectedDate.getTime()) {
          streak++;
          expectedDate.setDate(expectedDate.getDate() - 1);
        } else {
          break;
        }
      }

      return streak;
    } catch (error) {
      logger.error('Error calculating daily streak:', error);
      return 0;
    }
  }

  /**
   * Calculates total podiums (vote count) for a user
   */
  async calculateTotalPodiums(userId: number): Promise<number> {
    try {
      const count = await this.userBrandVotesRepository.count({
        where: { user: { id: userId } },
      });
      return count;
    } catch (error) {
      logger.error('Error calculating total podiums:', error);
      return 0;
    }
  }

  /**
   * Calculates unique brands count that user has voted for
   */
  async calculateVotedBrandsCount(userId: number): Promise<number> {
    try {
      const votes = await this.userBrandVotesRepository.find({
        where: { user: { id: userId } },
        relations: ['brand1', 'brand2', 'brand3'],
      });

      const uniqueBrandIds = new Set<number>();
      votes.forEach((vote) => {
        if (vote.brand1) uniqueBrandIds.add(vote.brand1.id);
        if (vote.brand2) uniqueBrandIds.add(vote.brand2.id);
        if (vote.brand3) uniqueBrandIds.add(vote.brand3.id);
      });

      return uniqueBrandIds.size;
    } catch (error) {
      logger.error('Error calculating voted brands count:', error);
      return 0;
    }
  }

  /**
   * Calculates user's favorite brand based on voting frequency
   */
  async calculateFavoriteBrand(userId: number): Promise<Brand | null> {
    try {
      const votes = await this.userBrandVotesRepository.find({
        where: { user: { id: userId } },
        relations: ['brand1', 'brand2', 'brand3'],
      });

      if (votes.length === 0) return null;

      const brandCounts = new Map<number, { brand: Brand; count: number }>();

      votes.forEach((vote) => {
        // Weight: 1st place = 3 points, 2nd place = 2 points, 3rd place = 1 point
        if (vote.brand1) {
          const existing = brandCounts.get(vote.brand1.id) || {
            brand: vote.brand1,
            count: 0,
          };
          brandCounts.set(vote.brand1.id, {
            brand: vote.brand1,
            count: existing.count + 3,
          });
        }
        if (vote.brand2) {
          const existing = brandCounts.get(vote.brand2.id) || {
            brand: vote.brand2,
            count: 0,
          };
          brandCounts.set(vote.brand2.id, {
            brand: vote.brand2,
            count: existing.count + 2,
          });
        }
        if (vote.brand3) {
          const existing = brandCounts.get(vote.brand3.id) || {
            brand: vote.brand3,
            count: 0,
          };
          brandCounts.set(vote.brand3.id, {
            brand: vote.brand3,
            count: existing.count + 1,
          });
        }
      });

      // Find brand with highest count
      let favoriteBrand: Brand | null = null;
      let maxCount = 0;

      for (const [_, { brand, count }] of brandCounts) {
        if (count > maxCount) {
          maxCount = count;
          favoriteBrand = brand;
        }
      }

      return favoriteBrand;
    } catch (error) {
      logger.error('Error calculating favorite brand:', error);
      return null;
    }
  }

  /**
   * Updates calculated fields for a specific user
   */
  async updateUserCalculatedFields(userId: number): Promise<void> {
    try {
      const [dailyStreak, totalPodiums, votedBrandsCount, favoriteBrand] =
        await Promise.all([
          this.calculateDailyStreak(userId),
          this.calculateTotalPodiums(userId),
          this.calculateVotedBrandsCount(userId),
          this.calculateFavoriteBrand(userId),
        ]);

      // Get current user to check maxDailyStreak
      const user = await this.userRepository.findOne({ where: { id: userId } });
      const currentMaxStreak = user?.maxDailyStreak ?? 0;
      const newMaxStreak = Math.max(currentMaxStreak, dailyStreak);

      await this.userRepository.update(userId, {
        dailyStreak,
        totalPodiums,
        votedBrandsCount,
        favoriteBrand: favoriteBrand ? { id: favoriteBrand.id } : null,
        maxDailyStreak: newMaxStreak,
      });

      logger.log(`Updated calculated fields for user ${userId}`);
    } catch (error) {
      logger.error(
        `Error updating calculated fields for user ${userId}:`,
        error,
      );
    }
  }

  /**
   * Backfills calculated fields for all existing users
   * Use this method to populate the new fields for existing users
   */
  async backfillAllUserCalculatedFields(): Promise<void> {
    try {
      logger.log('Starting backfill of calculated fields for all users...');

      const users = await this.userRepository.find({ select: ['id'] });
      logger.log(`Found ${users.length} users to process`);

      let processed = 0;
      for (const user of users) {
        await this.updateUserCalculatedFields(user.id);
        processed++;

        if (processed % 10 === 0) {
          logger.log(`Processed ${processed}/${users.length} users`);
        }
      }

      logger.log(`Backfill completed! Processed ${processed} users`);
    } catch (error) {
      logger.error('Error during backfill:', error);
      throw error;
    }
  }

  /**
   * Gets contextual transaction data based on user's daily state
   * Logic:
   * - If user hasn't voted: return null
   * - If user voted but hasn't shared: return vote transaction hash
   * - If user voted and shared but not claimed: return null (no tx to show)
   * - If user voted, shared, and claimed: return claim transaction hash + amount + cast hash
   */
  async getContextualTransactionData(fid: number): Promise<{
    transactionHash: string | null;
    transactionType: 'vote' | 'claim' | null;
    rewardAmount?: string;
    castHash?: string;
    day?: number;
  }> {
    try {
      // Calculate today's day number
      const now = Math.floor(Date.now() / 1000);
      const day = Math.floor(now / 86400);

      // Get today's vote record
      const vote = await this.userBrandVotesRepository.findOne({
        where: {
          user: { fid: fid },
          day: day,
        },
        relations: ['user'],
      });

      if (!vote) {
        // User hasn't voted today
        return {
          transactionHash: null,
          transactionType: null,
        };
      }

      const hasVoted = true;
      const hasShared = !!(vote.castHash && vote.shared);
      const hasClaimed = !!vote.claimedAt;

      if (hasVoted && !hasShared) {
        // User voted but hasn't shared - show vote transaction
        return {
          transactionHash: vote.transactionHash,
          transactionType: 'vote',
          day: vote.day,
        };
      } else if (hasVoted && hasShared && !hasClaimed) {
        // User voted and shared but not claimed - show no transaction
        return {
          transactionHash: null,
          transactionType: null,
        };
      } else if (hasVoted && hasShared && hasClaimed) {
        // User voted, shared, and claimed - show claim transaction, amount, and cast hash
        return {
          transactionHash: vote.claimTxHash,
          transactionType: 'claim',
          rewardAmount: vote.rewardAmount,
          castHash: vote.castHash,
          day: vote.day,
        };
      }

      // Fallback - shouldn't reach here
      return {
        transactionHash: null,
        transactionType: null,
      };
    } catch (error) {
      logger.error('Error getting contextual transaction data:', error);
      return {
        transactionHash: null,
        transactionType: null,
      };
    }
  }

  /**
   * Gets user's airdrop eligibility and snapshot information
   * @param fid User's Farcaster ID
   * @returns Airdrop eligibility data for fast frontend rendering
   */
  async getUserAirdropInfo(fid: number): Promise<{
    isEligible: boolean;
    airdropScore?: number;
    tokenAllocation?: number;
    percentage?: number;
    multipliers?: number;
    leaderboardPosition?: number;
    snapshotExists: boolean;
    hasClaimed: boolean;
    latestSnapshot?: {
      id: number;
      merkleRoot: string;
      totalUsers: number;
      totalTokens: string;
      snapshotDate: Date;
    };
  }> {
    try {
      // Check if user has airdrop score (is eligible)
      const airdropScore = await this.airdropScoreRepository.findOne({
        where: { fid },
      });

      // Get latest snapshot info
      const snapshotResults = await this.airdropSnapshotRepository.find({
        order: { createdAt: 'DESC' },
        select: [
          'id',
          'merkleRoot',
          'totalUsers',
          'totalTokens',
          'snapshotDate',
        ],
        take: 1,
      });
      const latestSnapshot =
        snapshotResults.length > 0 ? snapshotResults[0] : null;

      // Check if user has claimed on the smart contract
      let hasClaimed = false;
      try {
        if (this.airdropContractService) {
          hasClaimed = await this.airdropContractService.hasClaimed(fid);
        }
      } catch (error) {
        logger.error(`Error checking claim status for FID ${fid}:`, error);
        // If contract call fails, default to false but log the error
      }

      const response: {
        isEligible: boolean;
        hasClaimed: boolean;
        airdropScore?: number;
        tokenAllocation?: number;
        percentage?: number;
        leaderboardPosition?: number;
        snapshotExists: boolean;
        multipliers?: number;
        latestSnapshot?: {
          id: number;
          merkleRoot: string;
          totalUsers: number;
          totalTokens: string;
          snapshotDate: Date;
        };
      } = {
        isEligible: !!airdropScore,
        hasClaimed,
        snapshotExists: !!latestSnapshot,
        multipliers: airdropScore?.totalMultiplier,
        latestSnapshot: latestSnapshot
          ? {
              id: latestSnapshot.id,
              merkleRoot: latestSnapshot.merkleRoot,
              totalUsers: latestSnapshot.totalUsers,
              totalTokens: latestSnapshot.totalTokens,
              snapshotDate: latestSnapshot.snapshotDate,
            }
          : undefined,
      };

      if (airdropScore) {
        response.airdropScore = Number(airdropScore.finalScore);
        response.tokenAllocation = Number(airdropScore.tokenAllocation);
        response.percentage = Number(airdropScore.percentage);

        // Get leaderboard position (rank among all eligible users)
        const higherScores = await this.airdropScoreRepository.count({
          where: {
            finalScore: MoreThan(airdropScore.finalScore),
          },
        });
        response.leaderboardPosition = higherScores + 1;
      }

      return response;
    } catch (error) {
      logger.error('Error getting user airdrop info:', error);
      // Try to get claim status even if other checks fail
      let hasClaimed = false;
      try {
        if (this.airdropContractService) {
          hasClaimed = await this.airdropContractService.hasClaimed(fid);
        }
      } catch (claimError) {
        logger.error(
          `Error checking claim status for FID ${fid} in error handler:`,
          claimError,
        );
      }
      return {
        isEligible: false,
        hasClaimed,
        snapshotExists: false,
      };
    }
  }

  /**
   * Gets comprehensive user data with precise activity status for today
   */
  async getComprehensiveUserData(fid: number): Promise<{
    user: User;
    brndPowerLevel: number;
    lastVoteTimestamp: Date | null;
    votedToday: boolean;
    sharedVoteToday: boolean;
    claimedRewardsToday: boolean;
    dailyStreak: number;
    totalPodiums: number;
    votedBrandsCount: number;
    points: number;
    leaderboardPosition: number;
    favoriteBrand: Brand | null;
    todaysVoteStatus: {
      hasVoted: boolean;
      hasShared: boolean;
      hasClaimed: boolean;
      voteId: string | null;
      castHash: string | null;
      transactionHash: string | null;
      day: number;
    } | null;
    contextualTransaction: {
      transactionHash: string | null;
      transactionType: 'vote' | 'claim' | null;
      rewardAmount?: string;
      castHash?: string;
      day?: number;
    };
    airdrop: {
      isEligible: boolean;
      airdropScore?: number;
      tokenAllocation?: number;
      percentage?: number;
      leaderboardPosition?: number;
      snapshotExists: boolean;
      latestSnapshot?: {
        id: number;
        merkleRoot: string;
        totalUsers: number;
        totalTokens: string;
        snapshotDate: Date;
      };
    };
  }> {
    try {
      // Get user with all relations
      const user = await this.userRepository.findOne({
        where: { fid },
        relations: ['favoriteBrand'],
      });

      if (!user) {
        throw new Error('User not found');
      }

      // Get today's vote status, contextual transaction data, and airdrop info
      const [todaysVoteStatus, contextualTransaction, airdropInfo] =
        await Promise.all([
          this.getTodaysVoteStatus(fid),
          this.getContextualTransactionData(fid),
          this.getUserAirdropInfo(fid),
        ]);

      // Calculate precise boolean flags
      const votedToday = todaysVoteStatus?.hasVoted || false;
      const sharedVoteToday = todaysVoteStatus?.hasShared || false;
      const claimedRewardsToday = todaysVoteStatus?.hasClaimed || false;

      // Get leaderboard position
      await this.refreshLeaderboardCacheIfNeeded();
      const userIndex =
        this.leaderboardCache?.users.findIndex((u) => u.fid === fid) ?? -1;
      const leaderboardPosition = userIndex !== -1 ? userIndex + 1 : 0;

      return {
        user,
        brndPowerLevel: user.brndPowerLevel,
        lastVoteTimestamp: user.lastVoteTimestamp,
        votedToday,
        sharedVoteToday,
        claimedRewardsToday,
        dailyStreak: user.dailyStreak,
        totalPodiums: user.totalPodiums,
        votedBrandsCount: user.votedBrandsCount,
        points: user.points,
        leaderboardPosition,
        favoriteBrand: user.favoriteBrand,
        todaysVoteStatus,
        contextualTransaction,
        airdrop: airdropInfo,
      };
    } catch (error) {
      logger.error('Error getting comprehensive user data:', error);
      throw error;
    }
  }

  /**
   * Gets consolidated user profile data for the frontend
   */
  async getUserProfile(fid: number): Promise<{
    leaderboardPosition: number;
    currentPoints: number;
    dailyStreak: number;
    totalPodiums: number;
    favoriteBrand: {
      name: string;
      iconUrl: string;
    } | null;
    votedBrands: number;
    neynarScore: number;
  }> {
    try {
      // Get user by FID
      const user = await this.userRepository.findOne({
        where: { fid },
        relations: ['favoriteBrand'],
        select: [
          'id',
          'fid',
          'points',
          'dailyStreak',
          'totalPodiums',
          'votedBrandsCount',
          'neynarScore',
        ],
      });

      if (!user) {
        throw new Error('User not found');
      }

      // Get leaderboard position
      await this.refreshLeaderboardCacheIfNeeded();
      const userIndex =
        this.leaderboardCache?.users.findIndex((u) => u.fid === fid) ?? -1;
      const leaderboardPosition = userIndex !== -1 ? userIndex + 1 : 0;

      // Format favorite brand
      const favoriteBrand = user.favoriteBrand
        ? {
            name: user.favoriteBrand.name,
            iconUrl: user.favoriteBrand.imageUrl,
          }
        : null;

      // Fetch Neynar score if not set
      let neynarScore = user.neynarScore;
      if (neynarScore === 0.0) {
        try {
          logger.log(`🔍 [USER PROFILE] Fetching Neynar score for FID: ${fid}`);
          const neynarUserInfo = await this.getNeynarUserInfo(fid);

          if (neynarUserInfo) {
            // Calculate score: 1.0 if has power badge, 0.8 otherwise
            neynarScore = neynarUserInfo.power_badge ? 1.0 : 0.8;

            // Update user record with the calculated score
            await this.userRepository.update({ id: user.id }, { neynarScore });

            logger.log(
              `✅ [USER PROFILE] Updated Neynar score for FID ${fid}: ${neynarScore}`,
            );
          } else {
            logger.warn(
              `⚠️ [USER PROFILE] Could not fetch Neynar info for FID: ${fid}`,
            );
          }
        } catch (error) {
          logger.error(
            `❌ [USER PROFILE] Error fetching Neynar score for FID ${fid}:`,
            error,
          );
          // Continue with 0.0 if fetch fails
        }
      }

      return {
        leaderboardPosition,
        currentPoints: user.points,
        dailyStreak: user.dailyStreak,
        totalPodiums: user.totalPodiums,
        favoriteBrand,
        votedBrands: user.votedBrandsCount,
        neynarScore,
      };
    } catch (error) {
      logger.error('Error getting user profile:', error);
      throw error;
    }
  }

  /**
   * Fetches user info from Neynar API
   */
  private async getNeynarUserInfo(fid: number): Promise<any> {
    try {
      const apiKey = getConfig().neynar.apiKey.replace(/&$/, '');

      const response = await fetch(
        `https://api.neynar.com/v2/farcaster/user/bulk?fids=${fid}`,
        {
          headers: {
            accept: 'application/json',
            api_key: apiKey,
          },
        },
      );

      if (!response.ok) {
        throw new Error(
          `Neynar API error: ${response.status} ${response.statusText}`,
        );
      }

      const data = await response.json();
      return data?.users?.[0] || null;
    } catch (error) {
      logger.error('Error fetching Neynar user info:', error);
      return null;
    }
  }
}
