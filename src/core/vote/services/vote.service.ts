// Dependencies
import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';

// Models
import { UserBrandVotes } from '../../../models';

@Injectable()
export class VoteService {
  constructor(
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
  ) {}

  /**
   * Retrieves the votes by transaction hash.
   *
   * @param {string} transactionHash - The transaction hash (primary key) of the vote to retrieve.
   * @returns {Promise<UserBrandVotes>} A promise that resolves to an object of the user's votes.
   */
  async getVotesByTransactionHash(transactionHash: string): Promise<UserBrandVotes> {
    const userBrandVotes = await this.userBrandVotesRepository.findOne({
      select: ['transactionHash', 'brand1', 'brand2', 'brand3', 'date'],
      where: { transactionHash },
      relations: ['brand1', 'brand2', 'brand3'],
    });
    return userBrandVotes;
  }

  /**
   * @deprecated Use getVotesByTransactionHash instead
   */
  async getVotesById(id: string): Promise<UserBrandVotes> {
    return this.getVotesByTransactionHash(id);
  }
}
