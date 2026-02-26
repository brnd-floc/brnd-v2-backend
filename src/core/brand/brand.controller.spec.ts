import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from '@jest/globals';
import { HttpStatus } from '@nestjs/common';

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
  'src/utils/neynar',
  () => ({
    __esModule: true,
    default: class NeynarService {},
  }),
  { virtual: true },
);

import { logger } from '../../main';
import { BrandController } from './brand.controller';

describe('BrandController no-op compatibility and embed domain config', () => {
  const originalNodeEnv = process.env.NODE_ENV;
  const originalEmbedUrls = process.env.BRAND_SHARE_EMBED_URLS;
  const originalEmbedLegacy = process.env.BRAND_SHARE_EMBED_URLS_LEGACY;

  let controller: BrandController;

  beforeEach(() => {
    jest.clearAllMocks();
    process.env.NODE_ENV = 'test';
    delete process.env.BRAND_SHARE_EMBED_URLS;
    delete process.env.BRAND_SHARE_EMBED_URLS_LEGACY;

    controller = new BrandController(
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
    );
  });

  afterEach(() => {
    if (originalNodeEnv === undefined) {
      delete process.env.NODE_ENV;
    } else {
      process.env.NODE_ENV = originalNodeEnv;
    }

    if (originalEmbedUrls === undefined) {
      delete process.env.BRAND_SHARE_EMBED_URLS;
    } else {
      process.env.BRAND_SHARE_EMBED_URLS = originalEmbedUrls;
    }

    if (originalEmbedLegacy === undefined) {
      delete process.env.BRAND_SHARE_EMBED_URLS_LEGACY;
    } else {
      process.env.BRAND_SHARE_EMBED_URLS_LEGACY = originalEmbedLegacy;
    }
  });

  it('requestBrand remains no-op and logs deprecation warning', async () => {
    const res = {
      req: { headers: { 'x-request-id': 'req-123' } },
      status: jest.fn().mockReturnThis(),
      json: jest.fn().mockReturnValue({}),
    } as any;

    const result = await controller.requestBrand(
      { fid: 123 } as any,
      { name: 'Brand X' },
      res,
    );

    expect(res.status).toHaveBeenCalledWith(HttpStatus.OK);
    expect(res.json).toHaveBeenCalledWith({});
    expect(result).toBeDefined();
    expect((logger.warn as jest.Mock).mock.calls[0][0]).toContain(
      'endpoint=/brand-service/request',
    );
  });

  it('followBrand remains no-op and logs deprecation warning', async () => {
    const result = await controller.followBrand({ fid: 444 } as any, '11');

    expect(result).toBeUndefined();
    expect((logger.warn as jest.Mock).mock.calls[0][0]).toContain(
      'endpoint=/brand-service/11/follow',
    );
  });

  it('uses final share embed defaults in production', () => {
    process.env.NODE_ENV = 'production';

    const urls = (controller as any).getVoteShareEmbedBaseUrls();

    expect(urls).toEqual([
      'https://brnd.land',
      'https://www.brnd.land',
      'https://frame.brnd.land',
    ]);
  });

  it('uses final plus legacy share embed defaults in non-production', () => {
    process.env.NODE_ENV = 'development';

    const urls = (controller as any).getVoteShareEmbedBaseUrls();

    expect(urls).toEqual([
      'https://brnd.land',
      'https://www.brnd.land',
      'https://frame.brnd.land',
      'https://rebrnd.lat',
      'https://poiesis.anky.app',
      'https://brnd-v2-backend-production.up.railway.app',
    ]);
    expect((logger.warn as jest.Mock).mock.calls.length).toBe(0);
  });

  it('uses explicit BRAND_SHARE_EMBED_URLS literally when configured', () => {
    process.env.BRAND_SHARE_EMBED_URLS = 'https://x.example, https://y.example';
    process.env.BRAND_SHARE_EMBED_URLS_LEGACY = 'https://legacy.example';

    const urls = (controller as any).getVoteShareEmbedBaseUrls();

    expect(urls).toEqual(['https://x.example', 'https://y.example']);
  });

  it('warns in production when explicit embed URLs include legacy entries', () => {
    process.env.NODE_ENV = 'production';
    process.env.BRAND_SHARE_EMBED_URLS =
      'https://brnd.land,https://rebrnd.lat';

    const urls = (controller as any).getVoteShareEmbedBaseUrls();

    expect(urls).toEqual(['https://brnd.land', 'https://rebrnd.lat']);
    expect((logger.warn as jest.Mock).mock.calls.length).toBe(1);
    expect((logger.warn as jest.Mock).mock.calls[0][0]).toContain(
      'event=config_drift_detected',
    );
    expect((logger.warn as jest.Mock).mock.calls[0][0]).toContain(
      'component=brand.share_embed',
    );
    expect((logger.warn as jest.Mock).mock.calls[0][0]).toContain(
      'domainOrUrl=https://rebrnd.lat',
    );
  });

  it('does not warn in production when explicit embed URLs are final-only', () => {
    process.env.NODE_ENV = 'production';
    process.env.BRAND_SHARE_EMBED_URLS =
      'https://brnd.land,https://www.brnd.land,https://frame.brnd.land';

    const urls = (controller as any).getVoteShareEmbedBaseUrls();

    expect(urls).toEqual([
      'https://brnd.land',
      'https://www.brnd.land',
      'https://frame.brnd.land',
    ]);
    expect((logger.warn as jest.Mock).mock.calls.length).toBe(0);
  });
});
