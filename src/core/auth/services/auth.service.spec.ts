import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';

jest.mock('../../../main', () => ({
  logger: {
    warn: jest.fn(),
    error: jest.fn(),
    log: jest.fn(),
  },
}));

import { AuthService } from './auth.service';
import { logger } from '../../../main';

describe('AuthService', () => {
  const originalDomains = process.env.QUICKAUTH_ALLOWED_DOMAINS;
  const originalLegacyDomains = process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY;
  const originalNodeEnv = process.env.NODE_ENV;

  beforeEach(() => {
    jest.clearAllMocks();
    delete process.env.QUICKAUTH_ALLOWED_DOMAINS;
    delete process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY;
    process.env.NODE_ENV = 'test';
  });

  afterEach(() => {
    if (originalDomains === undefined) {
      delete process.env.QUICKAUTH_ALLOWED_DOMAINS;
    } else {
      process.env.QUICKAUTH_ALLOWED_DOMAINS = originalDomains;
    }

    if (originalLegacyDomains === undefined) {
      delete process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY;
    } else {
      process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY = originalLegacyDomains;
    }

    if (originalNodeEnv === undefined) {
      delete process.env.NODE_ENV;
    } else {
      process.env.NODE_ENV = originalNodeEnv;
    }
  });

  it('verifies token with first allowed domain', async () => {
    const service = new AuthService();
    const verifyJwt = (jest.fn() as any).mockResolvedValue({ sub: 123 });

    (service as any).farcasterClient = { verifyJwt };
    (service as any).quickAuthDomains = ['miniapp.brndland.com', 'brnd.land'];

    const payload = await service.verifyQuickAuthToken('token-1');

    expect(payload).toEqual({ sub: 123 });
    expect(verifyJwt).toHaveBeenCalledTimes(1);
    expect(verifyJwt).toHaveBeenCalledWith({
      token: 'token-1',
      domain: 'miniapp.brndland.com',
    });
    expect((logger.warn as jest.Mock).mock.calls.length).toBe(0);
  });

  it('falls back to second domain and logs warning', async () => {
    const service = new AuthService();
    const verifyJwt: any = jest.fn();
    verifyJwt.mockRejectedValueOnce(new Error('first failed'));
    verifyJwt.mockResolvedValueOnce({ sub: 456 });

    (service as any).farcasterClient = { verifyJwt };
    (service as any).quickAuthDomains = ['miniapp.brndland.com', 'brnd.land'];

    const payload = await service.verifyQuickAuthToken('token-2');

    expect(payload).toEqual({ sub: 456 });
    expect(verifyJwt).toHaveBeenCalledTimes(2);
    expect(verifyJwt).toHaveBeenNthCalledWith(1, {
      token: 'token-2',
      domain: 'miniapp.brndland.com',
    });
    expect(verifyJwt).toHaveBeenNthCalledWith(2, {
      token: 'token-2',
      domain: 'brnd.land',
    });
    expect((logger.warn as jest.Mock).mock.calls.length).toBe(1);
  });

  it('throws when all domains fail verification', async () => {
    const service = new AuthService();
    const verifyJwt: any = jest.fn();
    verifyJwt.mockRejectedValueOnce(new Error('a'));
    verifyJwt.mockRejectedValueOnce(new Error('b'));

    (service as any).farcasterClient = { verifyJwt };
    (service as any).quickAuthDomains = ['a.example', 'b.example'];

    await expect(service.verifyQuickAuthToken('token-3')).rejects.toThrow(
      'Token verification failed: b',
    );
    expect((logger.error as jest.Mock).mock.calls.length).toBe(1);
  });

  it('fails fast when QUICKAUTH_ALLOWED_DOMAINS is empty', () => {
    const service = new AuthService();
    process.env.QUICKAUTH_ALLOWED_DOMAINS = '   ,   ';

    expect(() => (service as any).loadQuickAuthDomains()).toThrow(
      'QUICKAUTH_ALLOWED_DOMAINS is empty',
    );
  });

  it('uses exact QUICKAUTH_ALLOWED_DOMAINS literal when configured', () => {
    const service = new AuthService();
    process.env.QUICKAUTH_ALLOWED_DOMAINS = 'custom.one,custom.two';
    process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY = 'legacy.one,legacy.two';
    process.env.NODE_ENV = 'production';

    expect((service as any).loadQuickAuthDomains()).toEqual([
      'custom.one',
      'custom.two',
    ]);
  });

  it('warns in production when explicit configured domains include legacy entries', () => {
    const service = new AuthService();
    process.env.NODE_ENV = 'production';
    process.env.QUICKAUTH_ALLOWED_DOMAINS = 'brnd.land,miniapp.anky.app';

    const domains = (service as any).loadQuickAuthDomains();

    expect(domains).toEqual(['brnd.land', 'miniapp.anky.app']);
    expect((logger.warn as jest.Mock).mock.calls.length).toBe(1);
    expect((logger.warn as jest.Mock).mock.calls[0][0]).toContain(
      'event=config_drift_detected',
    );
    expect((logger.warn as jest.Mock).mock.calls[0][0]).toContain(
      'component=auth.quickauth',
    );
    expect((logger.warn as jest.Mock).mock.calls[0][0]).toContain(
      'domainOrUrl=miniapp.anky.app',
    );
  });

  it('does not warn in production when configured domains are final-only', () => {
    const service = new AuthService();
    process.env.NODE_ENV = 'production';
    process.env.QUICKAUTH_ALLOWED_DOMAINS =
      'brnd.land,www.brnd.land,frame.brnd.land';

    const domains = (service as any).loadQuickAuthDomains();

    expect(domains).toEqual(['brnd.land', 'www.brnd.land', 'frame.brnd.land']);
    expect((logger.warn as jest.Mock).mock.calls.length).toBe(0);
  });

  it('uses final defaults only in production when env is unset', () => {
    const service = new AuthService();
    delete process.env.QUICKAUTH_ALLOWED_DOMAINS;
    delete process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY;
    process.env.NODE_ENV = 'production';

    expect((service as any).loadQuickAuthDomains()).toEqual([
      'brnd.land',
      'www.brnd.land',
      'frame.brnd.land',
    ]);
  });

  it('uses final + legacy defaults in non-production when env is unset', () => {
    const service = new AuthService();
    delete process.env.QUICKAUTH_ALLOWED_DOMAINS;
    delete process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY;
    process.env.NODE_ENV = 'development';

    expect((service as any).loadQuickAuthDomains()).toEqual([
      'brnd.land',
      'www.brnd.land',
      'frame.brnd.land',
      'miniapp.anky.app',
      'brndland.com',
      'miniapp.brndland.com',
      'api.brndland.com',
    ]);
    expect((logger.warn as jest.Mock).mock.calls.length).toBe(0);
  });

  it('uses final + configured legacy list in non-production', () => {
    const service = new AuthService();
    delete process.env.QUICKAUTH_ALLOWED_DOMAINS;
    process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY = 'legacy.a,legacy.b';
    process.env.NODE_ENV = 'staging';

    expect((service as any).loadQuickAuthDomains()).toEqual([
      'brnd.land',
      'www.brnd.land',
      'frame.brnd.land',
      'legacy.a',
      'legacy.b',
    ]);
  });
});
