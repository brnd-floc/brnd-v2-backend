import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { HttpStatus } from '@nestjs/common';

jest.mock('../../main', () => ({
  logger: { log: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));
jest.mock('src/main', () => ({
  logger: { log: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));
jest.mock('src/utils/ipfs.service', () => ({
  IpfsService: class IpfsService {},
}));
jest.mock('src/utils/neynar', () => ({
  __esModule: true,
  default: jest.fn(),
}));

import NeynarService from 'src/utils/neynar';
import { BrandController } from './brand.controller';

describe('daily reward recovery', () => {
  const fid = 123;
  const transactionHash = `0x${'1'.repeat(64)}`;
  const castHash = `0x${'2'.repeat(40)}`;
  const recipientAddress = `0x${'3'.repeat(40)}`;
  const day = 20763;
  const signature = {
    signature: '0xsignature',
    amount: '1000000000000000000000',
    nonce: 7,
    deadline: 1800000000,
    canClaim: true,
  };
  const body = {
    voteId: transactionHash,
    transactionHash,
    castHash,
    recipientAddress,
    castedFrom: 0,
  };

  let vote: any;
  let brandService: any;
  let userService: any;
  let rewardService: any;
  let neynar: any;
  let controller: BrandController;

  const response = () => ({
    status: jest.fn().mockReturnThis(),
    json: jest.fn().mockReturnThis(),
  });

  beforeEach(() => {
    jest.clearAllMocks();
    vote = {
      transactionHash,
      user: { fid },
      day,
      // Stored indexed day must win over a conflicting legacy date.
      date: new Date((day - 1) * 86400000),
      shared: true,
      shareVerified: false,
      castHash,
      claimedAt: null,
    };
    brandService = {
      getVoteByTransactionHash: (jest.fn() as any).mockImplementation(
        async () => vote,
      ),
      markVoteAsShared: (jest.fn() as any).mockImplementation(
        async (_tx: string, hash: string) => {
          vote.shared = true;
          vote.castHash = hash;
          return vote;
        },
      ),
    };
    userService = {
      getByFid: (jest.fn() as any).mockResolvedValue({
        id: 1,
        fid,
        points: 50,
      }),
      addPoints: (jest.fn() as any).mockResolvedValue({
        id: 1,
        fid,
        points: 53,
        brndPowerLevel: 1,
      }),
    };
    rewardService = {
      verifyShareForReward: (jest.fn() as any).mockImplementation(async () => {
        vote.shareVerified = true;
        return true;
      }),
      generateClaimSignature: (jest.fn() as any).mockResolvedValue(signature),
    };
    neynar = {
      getCastByHash: (jest.fn() as any).mockResolvedValue({
        hash: castHash,
        author: { fid },
        embeds: [{ url: `https://brnd.land/embeds/podium/${transactionHash}` }],
      }),
    };
    (NeynarService as unknown as jest.Mock).mockImplementation(() => neynar);
    controller = new BrandController(
      brandService,
      {} as any,
      userService,
      rewardService,
      {} as any,
      {} as any,
      {} as any,
    );
  });

  it('resumes a saved share using the original cast and indexed day without awarding points twice', async () => {
    const res = response();
    await controller.verifyShare(
      { sub: fid } as any,
      { ...body, castHash: '', transactionHash: undefined },
      res as any,
    );

    expect(brandService.getVoteByTransactionHash).toHaveBeenCalledWith(
      transactionHash,
      1,
    );
    expect(rewardService.verifyShareForReward).toHaveBeenCalledWith(
      fid,
      day,
      castHash,
    );
    expect(rewardService.generateClaimSignature).toHaveBeenCalledWith(
      fid,
      day,
      recipientAddress,
      castHash,
    );
    expect(userService.addPoints).not.toHaveBeenCalled();
    expect(brandService.markVoteAsShared).not.toHaveBeenCalled();
    expect(neynar.getCastByHash).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(HttpStatus.OK);
    expect(res.json).toHaveBeenCalledWith(
      expect.objectContaining({ verified: true, claimSignature: signature }),
    );
  });

  it('recovers when the first share saved successfully but reward verification failed', async () => {
    vote.shared = false;
    vote.castHash = null;
    rewardService.verifyShareForReward.mockResolvedValueOnce(false);

    const first = response();
    await controller.verifyShare({ sub: fid } as any, body, first as any);
    expect(first.status).toHaveBeenCalledWith(HttpStatus.SERVICE_UNAVAILABLE);
    expect(first.json).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          code: 'SHARE_VERIFICATION_UNAVAILABLE',
          retryable: true,
        }),
      }),
    );
    expect(rewardService.generateClaimSignature).not.toHaveBeenCalled();

    const retry = response();
    await controller.verifyShare({ sub: fid } as any, body, retry as any);
    expect(retry.status).toHaveBeenCalledWith(HttpStatus.OK);
    expect(rewardService.generateClaimSignature).toHaveBeenCalledWith(
      fid,
      day,
      recipientAddress,
      castHash,
    );
    expect(userService.addPoints).toHaveBeenCalledTimes(1);
    expect(brandService.markVoteAsShared).toHaveBeenCalledTimes(1);
    expect(neynar.getCastByHash).toHaveBeenCalledTimes(1);
  });

  it('returns a retryable 503 after a signature failure and succeeds on the same vote without sharing again', async () => {
    vote.shared = false;
    vote.castHash = null;
    rewardService.generateClaimSignature.mockRejectedValueOnce(
      new Error('RPC unavailable'),
    );

    const first = response();
    await controller.verifyShare({ sub: fid } as any, body, first as any);
    expect(first.status).toHaveBeenCalledWith(HttpStatus.SERVICE_UNAVAILABLE);
    expect(first.json).toHaveBeenCalledWith({
      action: 'verifyShare',
      data: {
        code: 'REWARD_CLAIM_UNAVAILABLE',
        message:
          'Your vote and share are saved. Reward claiming is temporarily unavailable. Please retry.',
        retryable: true,
      },
    });
    expect(vote.claimedAt).toBeNull();

    const retry = response();
    await controller.verifyShare({ sub: fid } as any, body, retry as any);
    expect(retry.status).toHaveBeenCalledWith(HttpStatus.OK);
    expect(rewardService.generateClaimSignature).toHaveBeenCalledTimes(2);
    expect(rewardService.verifyShareForReward).toHaveBeenCalledTimes(1);
    expect(userService.addPoints).toHaveBeenCalledTimes(1);
    expect(neynar.getCastByHash).toHaveBeenCalledTimes(1);
    expect(vote.transactionHash).toBe(transactionHash);
  });

  it('does not sign or award points again when the reward has already been claimed', async () => {
    vote.claimedAt = new Date();
    const res = response();
    await controller.verifyShare({ sub: fid } as any, body, res as any);

    expect(res.status).toHaveBeenCalledWith(HttpStatus.OK);
    expect(res.json).toHaveBeenCalledWith(
      expect.objectContaining({
        hasClaimed: true,
        canClaim: false,
        claimSignature: null,
        pointsAwarded: 0,
      }),
    );
    expect(rewardService.generateClaimSignature).not.toHaveBeenCalled();
    expect(rewardService.verifyShareForReward).not.toHaveBeenCalled();
    expect(userService.addPoints).not.toHaveBeenCalled();
  });

  it('does not query an arbitrary vote when transaction identification is missing', async () => {
    const res = response();
    await controller.verifyShare(
      { sub: fid } as any,
      { ...body, voteId: 'legacy-id', transactionHash: undefined },
      res as any,
    );

    expect(res.status).toHaveBeenCalledWith(HttpStatus.BAD_REQUEST);
    expect(brandService.getVoteByTransactionHash).not.toHaveBeenCalled();
    expect(rewardService.generateClaimSignature).not.toHaveBeenCalled();
  });

  it('returns a retryable response after one lookup when the vote is not indexed', async () => {
    brandService.getVoteByTransactionHash.mockResolvedValueOnce(null);
    const res = response();

    await controller.verifyShare({ sub: fid } as any, body, res as any);

    expect(res.status).toHaveBeenCalledWith(HttpStatus.SERVICE_UNAVAILABLE);
    expect(res.json).toHaveBeenCalledWith({
      action: 'verifyShare',
      data: expect.objectContaining({
        code: 'VOTE_INDEXING_PENDING',
        retryable: true,
      }),
    });
    expect(brandService.getVoteByTransactionHash).toHaveBeenCalledTimes(1);
    expect(brandService.getVoteByTransactionHash).toHaveBeenCalledWith(
      transactionHash,
      1,
    );
    expect(rewardService.generateClaimSignature).not.toHaveBeenCalled();
  });

  it('keeps ownership checks ahead of recovery', async () => {
    vote.user.fid = fid + 1;
    const res = response();
    await controller.verifyShare({ sub: fid } as any, body, res as any);

    expect(res.status).toHaveBeenCalledWith(HttpStatus.FORBIDDEN);
    expect(rewardService.verifyShareForReward).not.toHaveBeenCalled();
    expect(rewardService.generateClaimSignature).not.toHaveBeenCalled();
  });

  it('still verifies the cast author before saving a new share', async () => {
    vote.shared = false;
    vote.castHash = null;
    neynar.getCastByHash.mockResolvedValueOnce({
      author: { fid: fid + 1 },
      embeds: [],
    });
    const res = response();
    await controller.verifyShare({ sub: fid } as any, body, res as any);

    expect(res.status).toHaveBeenCalledWith(HttpStatus.FORBIDDEN);
    expect(brandService.markVoteAsShared).not.toHaveBeenCalled();
    expect(userService.addPoints).not.toHaveBeenCalled();
    expect(rewardService.generateClaimSignature).not.toHaveBeenCalled();
  });
});
