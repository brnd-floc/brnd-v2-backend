import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from '@jest/globals';

jest.mock('../../../main', () => ({
  logger: {
    log: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  },
}));
jest.mock('../../../models', () => ({
  User: class User {},
  UserBrandVotes: class UserBrandVotes {},
}));
jest.mock('./podium.service', () => ({
  PodiumService: class PodiumService {},
}));
jest.mock('viem', () => ({
  createPublicClient: jest.fn(),
  createWalletClient: jest.fn(),
  http: jest.fn(),
  encodeAbiParameters: jest.fn(),
  verifyTypedData: jest.fn(),
}));
jest.mock('viem/accounts', () => ({
  privateKeyToAccount: jest.fn(),
}));

import {
  createPublicClient,
  createWalletClient,
  http,
  verifyTypedData,
} from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { base } from 'viem/chains';
import { SignatureService } from './signature.service';

describe('SignatureService.generateRewardClaimSignature', () => {
  const envKeys = [
    'PRIVATE_KEY',
    'BASE_RPC_URL',
    'BRND_SEASON_2_ADDRESS',
  ] as const;
  const originalEnv = Object.fromEntries(
    envKeys.map((key) => [key, process.env[key]]),
  );
  const recipient = '0x1111111111111111111111111111111111111111';
  const signerAddress = '0x2222222222222222222222222222222222222222';
  const contractAddress = '0x3333333333333333333333333333333333333333';
  const castHash = '0x4444444444444444444444444444444444444444';
  const amount = '1000000000000000000000';
  const fid = 123;
  const day = 20763;
  const deadline = 1793966400;
  const rpcUrl = 'https://configured-rpc.example.test';

  let service: SignatureService;
  let vote: { claimedAt: Date | null };
  let voteRepository: {
    findOne: jest.Mock<(...args: any[]) => Promise<any>>;
    save: jest.Mock;
    update: jest.Mock;
  };
  let readContract: jest.Mock<(...args: any[]) => Promise<bigint>>;
  let signTypedData: jest.Mock<(...args: any[]) => Promise<string>>;

  beforeEach(() => {
    jest.resetAllMocks();
    // Synthetic fixtures; signing and all RPC calls are mocked.
    process.env.PRIVATE_KEY = `0x${'1'.repeat(64)}`;
    process.env.BASE_RPC_URL = rpcUrl;
    process.env.BRND_SEASON_2_ADDRESS = contractAddress;

    vote = { claimedAt: null };
    voteRepository = {
      findOne: jest
        .fn<(...args: any[]) => Promise<any>>()
        .mockResolvedValue(vote),
      save: jest.fn(),
      update: jest.fn(),
    };
    readContract = jest
      .fn<(...args: any[]) => Promise<bigint>>()
      .mockResolvedValue(BigInt(7));
    signTypedData = jest
      .fn<(...args: any[]) => Promise<string>>()
      .mockResolvedValue('0xsignature');

    jest.mocked(privateKeyToAccount).mockReturnValue({
      address: signerAddress,
    } as any);
    jest.mocked(http).mockImplementation((url) => ({ url }) as any);
    (createPublicClient as unknown as jest.Mock).mockReturnValue({
      readContract,
    });
    (createWalletClient as unknown as jest.Mock).mockReturnValue({
      signTypedData,
    });
    jest.mocked(verifyTypedData).mockResolvedValue(true);

    service = new SignatureService(
      {
        findOne: jest
          .fn<(...args: any[]) => Promise<any>>()
          .mockResolvedValue({ fid }),
      } as any,
      voteRepository as any,
      {} as any,
    );
  });

  afterEach(() => {
    for (const key of envKeys) {
      if (originalEnv[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = originalEnv[key];
      }
    }
  });

  const generateSignature = () =>
    service.generateRewardClaimSignature(
      recipient,
      fid,
      amount,
      day,
      castHash,
      deadline,
    );

  it('reads the nonce through the configured RPC and signs the original reward', async () => {
    const result = await generateSignature();

    expect(createPublicClient).toHaveBeenCalledWith({
      chain: base,
      transport: { url: rpcUrl },
    });
    expect(http).toHaveBeenCalledWith(rpcUrl);
    expect(readContract).toHaveBeenCalledWith(
      expect.objectContaining({
        address: contractAddress,
        functionName: 'rewardNonces',
        args: [recipient],
      }),
    );
    expect(signTypedData).toHaveBeenCalledWith(
      expect.objectContaining({
        domain: {
          name: 'BRNDSEASON2',
          version: '1',
          chainId: 8453,
          verifyingContract: contractAddress,
        },
        primaryType: 'RewardClaim',
        message: {
          recipient,
          amount: BigInt(amount),
          fid: BigInt(fid),
          day: BigInt(day),
          castHash,
          nonce: BigInt(7),
          deadline: BigInt(deadline),
        },
      }),
    );
    expect(result).toEqual({
      signature: '0xsignature',
      nonce: 7,
      canClaim: true,
    });
    expect(vote.claimedAt).toBeNull();
    expect(voteRepository.save).not.toHaveBeenCalled();
    expect(voteRepository.update).not.toHaveBeenCalled();
  });

  it('propagates RPC failure without claiming the reward and allows a fresh retry', async () => {
    const rpcError = new Error('RPC temporarily unavailable');
    readContract.mockRejectedValueOnce(rpcError);

    await expect(generateSignature()).rejects.toBe(rpcError);
    expect(signTypedData).not.toHaveBeenCalled();
    expect(vote.claimedAt).toBeNull();
    expect(voteRepository.save).not.toHaveBeenCalled();
    expect(voteRepository.update).not.toHaveBeenCalled();

    readContract.mockResolvedValueOnce(BigInt(8));
    await expect(generateSignature()).resolves.toEqual({
      signature: '0xsignature',
      nonce: 8,
      canClaim: true,
    });
    expect(readContract).toHaveBeenCalledTimes(2);
    expect(signTypedData).toHaveBeenCalledTimes(1);
    expect(signTypedData).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.objectContaining({
          nonce: BigInt(8),
          day: BigInt(day),
        }),
      }),
    );
    expect(vote.claimedAt).toBeNull();
  });

  it('rejects an already claimed reward before reading or signing', async () => {
    vote.claimedAt = new Date('2026-11-06T10:00:00Z');

    await expect(generateSignature()).rejects.toThrow(
      'Reward already claimed for this day',
    );
    expect(readContract).not.toHaveBeenCalled();
    expect(signTypedData).not.toHaveBeenCalled();
    expect(voteRepository.save).not.toHaveBeenCalled();
    expect(voteRepository.update).not.toHaveBeenCalled();
  });
});
