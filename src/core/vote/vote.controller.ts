// Dependencies
import { Controller, Get, Param } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';

// Services
import { VoteService } from './services';

// Models
import { UserBrandVotes } from '../../models';

@ApiTags('vote-service')
@Controller('vote-service')
export class VoteController {
  constructor(private readonly voteService: VoteService) {}

  /**
   * Retrieves a vote by its transaction hash.
   *
   * @param {string} transactionHash - The transaction hash (primary key) of the vote to retrieve.
   * @returns {Promise<Vote | undefined>} The vote entity or undefined if not found.
   */
  @Get('/:transactionHash')
  getVoteByTransactionHash(
    @Param('transactionHash') transactionHash: string,
  ): Promise<UserBrandVotes | undefined> {
    return this.voteService.getVotesByTransactionHash(transactionHash);
  }
}
