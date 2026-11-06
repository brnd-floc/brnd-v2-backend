import { beforeEach, describe, expect, it, jest } from '@jest/globals';

jest.mock('../../../main', () => ({
  logger: { log: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));
jest.mock('../../../models', () => ({
  User: class User {},
  UserBrandVotes: class UserBrandVotes {},
}));
jest.mock('./signature.service', () => ({
  SignatureService: class SignatureService {},
}));

import { RewardService } from './reward.service';

describe('RewardService concurrent claim recovery', () => {
  const fid = 123;
  const day = 20763;
  const transactionHash = `0x${'1'.repeat(64)}`;
  const castHash = `0x${'2'.repeat(40)}`;
  const claimTxHash = `0x${'3'.repeat(64)}`;
  const recipient = `0x${'4'.repeat(40)}`;
  const amount = '1000000000000000000000';
  const claimedAt = new Date('2026-11-06T10:00:00Z');

  let service: RewardService;
  let storedVote: Record<string, any>;
  let repository: {
    findOne: jest.Mock<(...args: any[]) => Promise<any>>;
    update: jest.Mock<(...args: any[]) => Promise<any>>;
    save: jest.Mock<(...args: any[]) => Promise<any>>;
  };
  let signatureService: {
    generateRewardClaimSignature: jest.Mock<(...args: any[]) => Promise<any>>;
  };

  const confirmClaim = () => {
    Object.assign(storedVote, { claimedAt, claimTxHash, pointsEarned: 18 });
  };

  beforeEach(() => {
    storedVote = {
      transactionHash,
      user: { fid },
      day,
      shared: true,
      shareVerified: true,
      shareVerifiedAt: new Date('2026-11-06T09:59:00Z'),
      castHash,
      rewardAmount: amount,
      brndPaidWhenCreatingPodium: 100,
      signatureGeneratedAt: null,
      nonce: 6,
      claimedAt: null,
      claimTxHash: null,
      pointsEarned: 3,
    };
    // Reads return snapshots, like TypeORM. A whole-entity save would overwrite
    // a concurrent indexer update; a partial update preserves unrelated fields.
    repository = {
      findOne: jest
        .fn<(...args: any[]) => Promise<any>>()
        .mockImplementation(async () => ({ ...storedVote })),
      update: jest
        .fn<(...args: any[]) => Promise<any>>()
        .mockImplementation(async (_where, patch) => {
          Object.assign(storedVote, patch);
          return { affected: 1 };
        }),
      save: jest
        .fn<(...args: any[]) => Promise<any>>()
        .mockImplementation(async (snapshot) => {
          Object.assign(storedVote, snapshot);
          return storedVote;
        }),
    };
    signatureService = {
      generateRewardClaimSignature: jest
        .fn<(...args: any[]) => Promise<any>>()
        .mockResolvedValue({
          signature: '0xsignature',
          nonce: 7,
          canClaim: true,
        }),
    };
    service = new RewardService(
      repository as any,
      {
        findOne: jest
          .fn<(...args: any[]) => Promise<any>>()
          .mockResolvedValue({ fid }),
      } as any,
      signatureService as any,
    );
  });

  it('preserves a claim indexed after the share recovery read', async () => {
    storedVote.shareVerified = false;
    repository.findOne.mockImplementationOnce(async () => {
      const snapshot = { ...storedVote };
      confirmClaim();
      return snapshot;
    });

    await expect(
      service.verifyShareForReward(fid, day, castHash),
    ).resolves.toBe(true);

    expect(repository.update).toHaveBeenCalledWith(
      { transactionHash },
      { shareVerified: true, shareVerifiedAt: expect.any(Date), castHash },
    );
    expect(repository.save).not.toHaveBeenCalled();
    expect(storedVote).toEqual(
      expect.objectContaining({
        claimedAt,
        claimTxHash,
        pointsEarned: 18,
        nonce: 6,
        rewardAmount: amount,
        shareVerified: true,
      }),
    );
  });

  it('preserves a claim indexed while a signature request was in progress', async () => {
    signatureService.generateRewardClaimSignature.mockImplementationOnce(
      async () => {
        confirmClaim();
        return { signature: '0xsignature', nonce: 7, canClaim: true };
      },
    );

    await expect(
      service.generateClaimSignature(fid, day, recipient, castHash),
    ).resolves.toEqual({
      signature: '0xsignature',
      amount,
      deadline: expect.any(Number),
      nonce: 7,
      canClaim: true,
    });

    expect(signatureService.generateRewardClaimSignature).toHaveBeenCalledWith(
      recipient,
      fid,
      amount,
      day,
      castHash,
      expect.any(Number),
    );
    expect(repository.update).toHaveBeenCalledWith(
      { transactionHash },
      { signatureGeneratedAt: expect.any(Date), nonce: 7 },
    );
    expect(repository.save).not.toHaveBeenCalled();
    expect(storedVote).toEqual(
      expect.objectContaining({ claimedAt, claimTxHash, pointsEarned: 18 }),
    );
  });

  it('does not report a verified share when no vote row was updated', async () => {
    repository.update.mockResolvedValueOnce({ affected: 0 });

    await expect(
      service.verifyShareForReward(fid, day, castHash),
    ).resolves.toBe(false);
    expect(repository.save).not.toHaveBeenCalled();
  });

  it('does not return a claim signature when its metadata could not be saved', async () => {
    repository.update.mockResolvedValueOnce({ affected: 0 });

    await expect(
      service.generateClaimSignature(fid, day, recipient, castHash),
    ).rejects.toThrow('Failed to persist reward claim signature');
    expect(storedVote.signatureGeneratedAt).toBeNull();
    expect(repository.save).not.toHaveBeenCalled();
  });

  it('does not change any vote fields when the signing service fails', async () => {
    const snapshot = { ...storedVote };
    const signingError = new Error('RPC unavailable');
    signatureService.generateRewardClaimSignature.mockRejectedValueOnce(
      signingError,
    );

    await expect(
      service.generateClaimSignature(fid, day, recipient, castHash),
    ).rejects.toBe(signingError);
    expect(repository.update).not.toHaveBeenCalled();
    expect(repository.save).not.toHaveBeenCalled();
    expect(storedVote).toEqual(snapshot);
  });

  it('keeps already claimed rewards ineligible', async () => {
    confirmClaim();

    await expect(
      service.generateClaimSignature(fid, day, recipient, castHash),
    ).rejects.toThrow('Cannot claim reward: Already claimed for this day');
    expect(
      signatureService.generateRewardClaimSignature,
    ).not.toHaveBeenCalled();
    expect(repository.update).not.toHaveBeenCalled();
    expect(repository.save).not.toHaveBeenCalled();
  });
});
