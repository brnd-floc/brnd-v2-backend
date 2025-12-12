// Dependencies
import {
  Body,
  Controller,
  Delete,
  Get,
  HttpStatus,
  Param,
  Patch,
  Post,
  Query,
  Res,
  UseGuards,
} from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { Response } from 'express';

// Services
import { UserService } from './services';

// Security
import {
  AdminGuard,
  AuthorizationGuard,
  QuickAuthPayload,
} from '../../security/guards';
import { Session } from '../../security/decorators';

// Models
import { User } from '../../models';

// Utils
import { hasError, hasResponse } from '../../utils';

@ApiTags('user-service')
@Controller('user-service')
export class UserController {
  constructor(private readonly userService: UserService) {}

  /**
   * Retrieves a user by their ID.
   *
   * @param {User['id']} id - The ID of the user to retrieve.
   * @returns {Promise<User>} The user with the specified ID.
   */
  @Get('/user/:id')
  getUserById(@Param('id') id: User['id']) {
    return this.userService.getById(id, [
      'id',
      'username',
      'photoUrl',
      'points',
      'createdAt',
    ]);
  }

  /**
   * Updates a user by their ID.
   *
   * @param {User['id']} id - The ID of the user to update.
   * @param {Partial<User>} data - The data to update the user with.
   * @returns {Promise<User>} The updated user.
   */
  @Patch('/user/:id')
  @UseGuards(AdminGuard)
  updateUser(@Param('id') id: User['id'], @Body('body') data: Partial<User>) {
    return this.userService.update(id, data);
  }

  /**
   * Deletes a user by their ID.
   *
   * @param {User['id']} id - The ID of the user to delete.
   * @returns {Promise<boolean>} Returns true if the user was successfully deleted.
   * @throws {Error} If the user with the specified ID is not found.
   */
  @Delete('/user/:id')
  @UseGuards(AdminGuard)
  deleteUser(@Param('id') id: User['id']) {
    return this.userService.delete(id);
  }

  /**
   * Retrieves the vote history of a user.
   *
   * @param {User['id']} id - The ID of the user whose vote history is to be retrieved.
   * @param {number} pageId - The page number for pagination.
   * @param {number} limit - The number of records per page.
   * @param {Response} res - The response object.
   * @returns {Promise<Response>} The response containing the user's vote history.
   */
  @Get('/user/:id/vote-history')
  async getUserVoteHistory(
    @Param('id') id: User['id'],
    @Query('pageId') pageId: number,
    @Query('limit') limit: number,
    @Res() res: Response,
  ) {
    const history = await this.userService.getVotesHistory(id, pageId, limit);

    return hasResponse(res, history);
  }

  /**
   * Retrieves the votes of a user for a specific date.
   *
   * @param session - The current user session.
   * @param unixDate - The date in Unix timestamp format.
   * @param res - The response object.
   * @returns A list of votes with details including id, date, position, and brand information.
   */
  @Get('/votes/:unixDate')
  @UseGuards(AuthorizationGuard)
  async getVotes(
    @Session() session: QuickAuthPayload, // CHANGE from User to QuickAuthPayload
    @Param('unixDate') unixDate: number,
    @Res() res: Response,
  ) {
    try {
      console.log(
        `📅 [UserController] Getting votes for FID: ${session.sub}, date: ${unixDate}`,
      );

      // Find user by FID from the authenticated session
      const user = await this.userService.getByFid(session.sub);
      if (!user) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getVotes',
          'User not found. Please refresh the app.',
        );
      }

      const vote = await this.userService.getUserVotes(user.id, unixDate);
      return hasResponse(res, vote);
    } catch (error) {
      console.error('❌ [UserController] Error getting votes:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getVotes',
        'Failed to retrieve votes',
      );
    }
  }

  @Post('/share-frame')
  @UseGuards(AuthorizationGuard)
  async addPointsForShareFrame(@Session() session: User, @Res() res: Response) {
    const response = await this.userService.addPointsForShareFrame(session.id);
    hasResponse(res, response);
  }

  /**
   * Retrieves the current authenticated user's personal brand rankings.
   * Shows all brands the user has voted for, ranked by total points earned.
   * Uses the auth guard to identify the user by their FID from the JWT token.
   *
   * @param {QuickAuthPayload} session - The authenticated user session from JWT
   * @param {Response} res - The response object
   * @returns {Promise<Response>} The response containing the user's brand rankings
   */
  @Get('/brands')
  @UseGuards(AuthorizationGuard)
  async getUserBrands(
    @Session() session: QuickAuthPayload,
    @Res() res: Response,
  ) {
    try {
      console.log(
        `🏆 [UserController] Getting brand rankings for FID: ${session.sub}`,
      );

      // Find user by FID from the authenticated session
      const user = await this.userService.getByFid(session.sub);
      if (!user) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getUserBrands',
          'User not found. Please refresh the app.',
        );
      }

      console.log(
        `🏆 [UserController] Found user: ${user.username} (ID: ${user.id})`,
      );

      // Get the user's personal brand rankings
      const userBrands = await this.userService.getUserBrands(user.id);

      return hasResponse(res, userBrands);
    } catch (error) {
      console.error('❌ [UserController] Error getting user brands:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getUserBrands',
        'Failed to retrieve brand rankings',
      );
    }
  }

  /**
   * Retrieves the current authenticated user's vote history.
   * Uses the auth guard to identify the user by their FID from the JWT token.
   *
   * @param {QuickAuthPayload} session - The authenticated user session from JWT
   * @param {number} pageId - The page number for pagination
   * @param {number} limit - The number of records per page
   * @param {Response} res - The response object
   * @returns {Promise<Response>} The response containing the user's vote history
   */
  @Get('/my-vote-history')
  @UseGuards(AuthorizationGuard)
  async getMyVoteHistory(
    @Session() session: QuickAuthPayload, // Use QuickAuthPayload instead of User
    @Query('pageId') pageId: number = 1,
    @Query('limit') limit: number = 15,
    @Res() res: Response,
  ) {
    try {
      console.log(
        `📊 [UserController] Getting vote history for FID: ${session.sub}`,
      );

      // Find user by FID from the authenticated session
      const user = await this.userService.getByFid(session.sub);
      if (!user) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getMyVoteHistory',
          'User not found. Please refresh the app.',
        );
      }

      console.log(
        `📊 [UserController] Found user: ${user.username} (ID: ${user.id})`,
      );

      // Get the user's vote history using their database ID
      const history = await this.userService.getVotesHistory(
        user.id,
        pageId,
        limit,
      );

      return hasResponse(res, history);
    } catch (error) {
      console.error('❌ [UserController] Error getting vote history:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getMyVoteHistory',
        'Failed to retrieve vote history',
      );
    }
  }

  @Get('/leaderboard')
  @UseGuards(AuthorizationGuard)
  async getLeaderboard(
    @Session() session: QuickAuthPayload,
    @Query('page') page: number = 1,
    @Query('limit') limit: number = 50,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      console.log(
        `🏆 [UserController] Getting leaderboard for user FID: ${session.sub}`,
      );

      const validatedPage = Math.max(1, Number(page) || 1);
      const validatedLimit = Math.min(100, Math.max(10, Number(limit) || 50));

      const leaderboard = await this.userService.getLeaderboard(
        validatedPage,
        validatedLimit,
        session.sub,
      );

      return hasResponse(res, leaderboard);
    } catch (error) {
      console.error('❌ [UserController] Error getting leaderboard:', error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getLeaderboard',
        'Failed to retrieve leaderboard',
      );
    }
  }

  /**
   * Gets consolidated user profile data including leaderboard position,
   * points, streak, podiums, favorite brand, and voting statistics.
   */
  @Get('/profile')
  @UseGuards(AuthorizationGuard)
  async getUserProfile(
    @Session() session: QuickAuthPayload,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      console.log(
        `👤 [UserController] Getting profile for user FID: ${session.sub}`,
      );

      const profile = await this.userService.getUserProfile(session.sub);

      return hasResponse(res, profile);
    } catch (error) {
      console.error('❌ [UserController] Error getting user profile:', error);

      if (error.message === 'User not found') {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getUserProfile',
          'User not found. Please refresh the app.',
        );
      }

      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getUserProfile',
        'Failed to retrieve user profile',
      );
    }
  }

  /**
   * Backfills calculated profile fields for all existing users (ADMIN ONLY).
   * This is a one-time operation to populate the new profile fields.
   */
  // TODO: ACTIVATE THIS WITH THE PROD DATA

  // @Get('/dev/backfill-profile-data')
  // async backfillProfileData(@Res() res: Response): Promise<Response> {
  //   try {
  //     console.log(
  //       `🔄 [UserController] Admin starting profile data backfill...`,
  //     );

  //     await this.userService.backfillAllUserCalculatedFields();

  //     console.log(
  //       `✅ [UserController] Profile data backfill completed successfully`,
  //     );

  //     return hasResponse(res, {
  //       message: 'Profile data backfill completed successfully',
  //       timestamp: new Date().toISOString(),
  //     });
  //   } catch (error) {
  //     console.error(
  //       '❌ [UserController] Error during profile data backfill:',
  //       error,
  //     );
  //     return hasError(
  //       res,
  //       HttpStatus.INTERNAL_SERVER_ERROR,
  //       'backfillProfileData',
  //       `Backfill failed: ${error.message}`,
  //     );
  //   }
  // }
}
