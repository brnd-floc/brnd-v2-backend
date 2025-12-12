// Dependencies
import { Controller, Get, Post, Req, Res, UseGuards } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { Response } from 'express';

// Services
import { UserService } from '../user/services';

// Security
import { AuthorizationGuard, QuickAuthPayload } from '../../security/guards';
import { Session } from '../../security/decorators';

import { logger } from '../../main';

// Utils
import { hasResponse, hasError, HttpStatus } from '../../utils';
import NeynarService from 'src/utils/neynar';

/**
 * Authentication controller for Farcaster miniapp integration.
 *
 * This controller handles user authentication and profile management using
 * Farcaster's QuickAuth system. The design is optimized for miniapp contexts
 * where users are implicitly authenticated through the Farcaster platform.
 *
 * Key architectural decisions:
 * - No explicit login/registration flow (handled automatically in /me)
 * - QuickAuth JWT tokens are verified but not regenerated
 * - User records are created/updated transparently on first access
 * - Logout only clears cookies (tokens remain valid until expiration)
 */
@ApiTags('auth-service')
@Controller('auth-service')
export class AuthController {
  constructor(private readonly userService: UserService) {}

  /**
   * Retrieves current user information with automatic user provisioning.
   *
   * This endpoint serves as the primary authentication mechanism for the miniapp.
   * It leverages Farcaster's QuickAuth system where users are always authenticated
   * within the miniapp context, eliminating the need for separate login flows.
   *
   * The endpoint handles:
   * 1. QuickAuth JWT token validation (via AuthorizationGuard)
   * 2. User lookup by FID from the verified token payload
   * 3. Automatic user record creation for first-time users
   * 4. Profile updates when user data is provided
   * 5. Daily voting status calculation for UI state management
   *
   * @param session - Verified QuickAuth JWT payload containing user FID and address
   * @param res - HTTP response object
   * @returns User profile data including voting status, points, and onboarding state
   */
  @Get('/me')
  @UseGuards(AuthorizationGuard)
  async getMe(@Session() session: QuickAuthPayload, @Res() res: Response) {
    try {
      logger.log('Processing user profile request for FID:', session.sub);

      // Check if user exists first
      let user = await this.userService.getByFid(session.sub);
      let isNewUser = false;

      if (!user) {
        // Create new user if doesn't exist
        logger.log('Creating new user record for FID:', session.sub);
        const neynar = new NeynarService();
        const neynarUser = await neynar.getUserByFid(session.sub);

        const { user: newUser, isCreated } = await this.userService.upsert(
          session.sub,
          {
            username: neynarUser.username,
            photoUrl: neynarUser.pfp_url,
            points: 0,
            brndPowerLevel: 1, // Default power level
            address: null,
            banned: false,
            powerups: 0,
            verified: false,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
        );

        user = newUser;
        isNewUser = isCreated;
      }

      // Get comprehensive user data with all precise information
      const userData = await this.userService.getComprehensiveUserData(
        session.sub,
      );

      // Get today's vote with brands for backward compatibility
      let todaysVote = null;
      if (userData.votedToday && userData.todaysVoteStatus?.voteId) {
        try {
          todaysVote = await this.userService.getTodaysVoteWithBrands(user.id);
        } catch (error) {
          logger.warn("Could not fetch today's vote details:", error);
        }
      }

      // Comprehensive response with all precise user data
      const responseData = {
        // Basic user info
        fid: userData.user.fid.toString(),
        username: userData.user.username,
        photoUrl: userData.user.photoUrl,
        address: userData.user.address,
        banned: userData.user.banned,
        verified: userData.user.verified,
        powerups: userData.user.powerups,

        // Points and levels
        points: userData.points,
        brndPowerLevel: userData.brndPowerLevel,
        leaderboardPosition: userData.leaderboardPosition,

        // Activity tracking
        lastVoteTimestamp: userData.lastVoteTimestamp,
        dailyStreak: userData.dailyStreak,
        totalPodiums: userData.totalPodiums,
        totalVotes: userData.user.totalVotes,
        votedBrandsCount: userData.votedBrandsCount,

        // Today's precise status (boolean flags)
        votedToday: userData.votedToday,
        sharedVoteToday: userData.sharedVoteToday,
        claimedRewardsToday: userData.claimedRewardsToday,

        // Contextual transaction data based on user's daily state
        contextualTransaction: userData.contextualTransaction,

        // Detailed today's vote status
        todaysVoteStatus: userData.todaysVoteStatus,

        // Legacy fields for backward compatibility
        hasVotedToday: userData.votedToday,
        hasSharedToday: userData.sharedVoteToday,
        todaysVote,

        // Favorite brand
        favoriteBrand: userData.favoriteBrand
          ? {
              id: userData.favoriteBrand.id,
              name: userData.favoriteBrand.name,
              imageUrl: userData.favoriteBrand.imageUrl,
            }
          : null,

        // Airdrop eligibility info
        // isEligible: true if user has an airdrop score
        // snapshotExists: true if an airdrop snapshot has been created
        // If snapshotExists: true and isEligible: false, user is not eligible
        airdrop: userData.airdrop,

        // Meta
        createdAt: userData.user.createdAt,
        updatedAt: userData.user.updatedAt,
        isNewUser,
      };
      console.log(
        'FINISHING THE CALL TO THE ME ENDPOINT AND SENDING BACK THIS RESPONSE DATA: ',
        JSON.stringify(responseData, null, 2),
      );

      return hasResponse(res, responseData);
    } catch (error) {
      logger.error('Failed to process user profile request:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getMe',
        'Unable to retrieve user profile.',
      );
    }
  }

  /**
   * Clears authentication cookies for logout functionality.
   *
   * Note: This endpoint only clears server-side cookies. QuickAuth tokens
   * remain valid until their expiration time since they are stateless JWTs.
   * Frontend applications should discard tokens locally for complete logout.
   *
   * @param req - Incoming HTTP request (used by guard for authentication)
   * @param res - HTTP response object for cookie manipulation
   * @returns Success confirmation
   */
  @Post('/logout')
  @UseGuards(AuthorizationGuard)
  async logOut(@Req() req: Request, @Res() res: Response) {
    try {
      res.clearCookie('Authorization');
      return hasResponse(res, 'Successfully logged out.');
    } catch (error) {
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'logOut',
        'An unexpected error occurred during logout.',
      );
    }
  }
}
