import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
  jest,
} from '@jest/globals';
import {
  ForbiddenException,
  HttpException,
  ServiceUnavailableException,
} from '@nestjs/common';

jest.mock('../../main', () => ({
  logger: {
    log: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
  },
}));
jest.mock(
  'src/main',
  () => ({
    logger: {
      log: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    },
  }),
  { virtual: true },
);
jest.mock(
  'src/utils/ipfs.service',
  () => ({
    IpfsService: class IpfsService {},
  }),
  { virtual: true },
);
jest.mock(
  'src/core/embeds/services/podium.service',
  () => ({
    PodiumService: class PodiumService {},
  }),
  { virtual: true },
);

import { BlockchainController } from './blockchain.controller';

describe('BlockchainController.claimPodiumSignature', () => {
  const originalIpfsTimeout = process.env.PODIUM_IPFS_TIMEOUT_MS;
  const originalIpfsRetries = process.env.PODIUM_IPFS_RETRIES;
  const originalRateLimitEnabled =
    process.env.PODIUM_CLAIM_RATE_LIMIT_ENABLED;
  const originalRateLimitWindow = process.env.PODIUM_CLAIM_RATE_LIMIT_WINDOW_MS;
  const originalRateLimitMaxPerFid =
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_FID;
  const originalRateLimitMaxPerIp =
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_IP;
  const originalRateLimitBlock = process.env.PODIUM_CLAIM_RATE_LIMIT_BLOCK_MS;

  const session = { sub: 123 } as any;
  const body = {
    walletAddress: '0x123',
    brandIds: [1, 2, 3],
    deadline: Math.floor(Date.now() / 1000) + 3600,
  } as any;
  const makeRequest = (
    ip = '1.1.1.1',
    xForwardedFor?: string,
  ): any => ({
    ip,
    headers: xForwardedFor ? { 'x-forwarded-for': xForwardedFor } : {},
  });

  let podiumService: any;
  let signatureService: any;
  let ipfsService: any;
  let controller: BlockchainController;

  beforeEach(() => {
    process.env.PODIUM_IPFS_TIMEOUT_MS = '5000';
    process.env.PODIUM_IPFS_RETRIES = '0';
    process.env.PODIUM_CLAIM_RATE_LIMIT_ENABLED = 'true';
    process.env.PODIUM_CLAIM_RATE_LIMIT_WINDOW_MS = '60000';
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_FID = '100';
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_IP = '100';
    process.env.PODIUM_CLAIM_RATE_LIMIT_BLOCK_MS = '120000';

    podiumService = {
      checkClaimEligibility: (jest.fn() as any).mockResolvedValue({
        eligible: true,
      }),
      checkBrndBalance: (jest.fn() as any).mockResolvedValue({
        sufficient: true,
        balanceWei: BigInt(2),
        requiredWei: BigInt(1),
      }),
      generateAndUploadPodiumImage: jest.fn(),
      getBrandNames: (jest.fn() as any).mockResolvedValue(['A', 'B', 'C']),
    };
    (podiumService.generateAndUploadPodiumImage as any).mockResolvedValue(
      'ipfs://image-uri',
    );

    signatureService = {
      generateClaimPodiumSignature: (jest.fn() as any).mockResolvedValue(
        '0xsig',
      ),
    };

    ipfsService = {
      uploadJsonToIpfs: (jest.fn() as any).mockResolvedValue(
        'ipfs://metadata-uri',
      ),
    };

    controller = new BlockchainController(
      {} as any,
      {} as any,
      signatureService,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      podiumService,
      ipfsService,
    );
  });

  afterEach(() => {
    process.env.PODIUM_IPFS_TIMEOUT_MS = originalIpfsTimeout;
    process.env.PODIUM_IPFS_RETRIES = originalIpfsRetries;
    process.env.PODIUM_CLAIM_RATE_LIMIT_ENABLED = originalRateLimitEnabled;
    process.env.PODIUM_CLAIM_RATE_LIMIT_WINDOW_MS = originalRateLimitWindow;
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_FID =
      originalRateLimitMaxPerFid;
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_IP = originalRateLimitMaxPerIp;
    process.env.PODIUM_CLAIM_RATE_LIMIT_BLOCK_MS = originalRateLimitBlock;
  });

  it('returns success payload with expected keys', async () => {
    const response = await controller.claimPodiumSignature(
      session,
      body,
      makeRequest(),
    );

    expect(response).toEqual({
      signature: '0xsig',
      price: '1000000000000000000000000',
      eligible: true,
      reason: null,
      metadataURI: 'ipfs://metadata-uri',
    });
  });

  it('returns 503 PODIUM_METADATA_UNAVAILABLE when image generation fails', async () => {
    podiumService.generateAndUploadPodiumImage.mockRejectedValueOnce(
      new Error('ipfs down'),
    );

    let thrown: ServiceUnavailableException | null = null;
    try {
      await controller.claimPodiumSignature(session, body, makeRequest());
    } catch (error) {
      thrown = error as ServiceUnavailableException;
    }

    expect(thrown).toBeInstanceOf(ServiceUnavailableException);
    const response = thrown!.getResponse() as any;
    expect(response.error).toBe('PODIUM_METADATA_UNAVAILABLE');
  });

  it('returns 503 PODIUM_METADATA_UNAVAILABLE when metadata upload fails', async () => {
    ipfsService.uploadJsonToIpfs.mockRejectedValue(new Error('pinata fail'));

    await expect(
      controller.claimPodiumSignature(session, body, makeRequest()),
    ).rejects.toBeInstanceOf(ServiceUnavailableException);
  });

  it('preserves ForbiddenException when user is not eligible', async () => {
    podiumService.checkClaimEligibility.mockResolvedValueOnce({
      eligible: false,
      reason: 'not eligible',
    });

    await expect(
      controller.claimPodiumSignature(session, body, makeRequest()),
    ).rejects.toBeInstanceOf(ForbiddenException);
  });

  it('blocks when requests exceed per-FID limit', async () => {
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_FID = '1';
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_IP = '50';
    controller = new BlockchainController(
      {} as any,
      {} as any,
      signatureService,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      podiumService,
      ipfsService,
    );

    const req = makeRequest('3.3.3.3');
    await controller.claimPodiumSignature(session, body, req);

    let thrown: HttpException | null = null;
    try {
      await controller.claimPodiumSignature(session, body, req);
    } catch (error) {
      thrown = error as HttpException;
    }

    expect(thrown).toBeInstanceOf(HttpException);
    expect(thrown!.getStatus()).toBe(429);
    const response = thrown!.getResponse() as any;
    expect(response.error).toBe('RATE_LIMITED');
    expect(response.retryAfterMs).toBeGreaterThan(0);
  });

  it('blocks when requests exceed per-IP limit', async () => {
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_FID = '50';
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_IP = '1';
    controller = new BlockchainController(
      {} as any,
      {} as any,
      signatureService,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      podiumService,
      ipfsService,
    );

    const req = makeRequest('4.4.4.4');
    await controller.claimPodiumSignature({ sub: 111 } as any, body, req);
    await expect(
      controller.claimPodiumSignature({ sub: 222 } as any, body, req),
    ).rejects.toBeInstanceOf(HttpException);
  });

  it('does not block when rate-limit is disabled', async () => {
    process.env.PODIUM_CLAIM_RATE_LIMIT_ENABLED = 'false';
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_FID = '1';
    process.env.PODIUM_CLAIM_RATE_LIMIT_MAX_PER_IP = '1';
    controller = new BlockchainController(
      {} as any,
      {} as any,
      signatureService,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      podiumService,
      ipfsService,
    );

    const req = makeRequest('5.5.5.5');
    await expect(
      controller.claimPodiumSignature(session, body, req),
    ).resolves.toBeDefined();
    await expect(
      controller.claimPodiumSignature(session, body, req),
    ).resolves.toBeDefined();
  });
});
