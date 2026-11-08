import { describe, expect, it, jest } from '@jest/globals';

jest.mock('../../../main', () => ({
  logger: { log: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));
jest.mock('../../../models', () => ({
  Brand: class Brand {},
  User: class User {},
  UserBrandVotes: class UserBrandVotes {},
}));
jest.mock('../../user/services', () => ({ UserService: class UserService {} }));
jest.mock('../../brand/services', () => ({
  BrandService: class BrandService {},
}));
jest.mock('./blockchain.service', () => ({
  BlockchainService: class BlockchainService {},
}));
jest.mock('./repeat-fee.service', () => ({
  RepeatFeeService: class RepeatFeeService {},
}));
jest.mock('src/core/embeds/services/podium.service', () => ({
  PodiumService: class PodiumService {},
}));

import { IndexerService } from './indexer.service';

describe('claim leaderboard points', () => {
  it('rolls the claim back when credit fails and credits a successful retry once', async () => {
    const claim = {
      id: 'claim',
      recipient: '0xrecipient',
      fid: 42,
      amount: '1000',
      day: '20765',
      castHash: '0xcast',
      caller: '0xcaller',
      blockNumber: '20',
      transactionHash: '0xclaim',
      timestamp: '1794096000',
    };
    const vote = { transactionHash: '0xvote', user: { id: 5, fid: 42 } };
    let storedClaim: string | undefined;
    const update = jest
      .fn<
        (
          where: unknown,
          patch: { claimTxHash: string },
        ) => Promise<{ affected: number }>
      >()
      .mockImplementation(async (_where, patch) => {
        if (storedClaim) return { affected: 0 };
        storedClaim = patch.claimTxHash;
        return { affected: 1 };
      });
    const manager = { getRepository: () => ({ update }) };
    const points = jest
      .fn<() => Promise<void>>()
      .mockRejectedValueOnce(new Error('write failed'))
      .mockResolvedValue(undefined);
    const invalidateLeaderboardCache = jest.fn();
    const votes = {
      findOne: async ({ where }: { where: { claimTxHash?: string } }) =>
        where.claimTxHash ? (storedClaim ? vote : null) : vote,
      manager: {
        async transaction(
          run: (transaction: typeof manager) => Promise<unknown>,
        ) {
          const before = storedClaim;
          try {
            return await run(manager);
          } catch (error) {
            storedClaim = before;
            throw error;
          }
        },
      },
    };
    const service = new IndexerService(
      {} as any,
      {} as any,
      votes as any,
      { addPoints: points, invalidateLeaderboardCache } as any,
      {} as any,
      {} as any,
      {
        getUserInfoFromContractByFid: async () => ({ brndPowerLevel: 4 }),
      } as any,
      {} as any,
    );

    await expect(service.handleRewardClaimSubmission(claim)).rejects.toThrow(
      'write failed',
    );
    expect(storedClaim).toBeUndefined();
    expect(invalidateLeaderboardCache).not.toHaveBeenCalled();

    await service.handleRewardClaimSubmission(claim);
    await service.handleRewardClaimSubmission(claim);

    expect(storedClaim).toBe(claim.transactionHash);
    expect(update).toHaveBeenCalledTimes(2);
    expect(points).toHaveBeenCalledTimes(2);
    expect(points).toHaveBeenLastCalledWith(5, 12, manager);
    expect(invalidateLeaderboardCache).toHaveBeenCalledTimes(1);
  });
});
