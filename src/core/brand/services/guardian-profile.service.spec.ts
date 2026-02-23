import { GuardianProfileService } from './guardian-profile.service';
import { describe, it, expect } from '@jest/globals';

describe('GuardianProfileService', () => {
  it('resolves guardian from founderFid when available', async () => {
    const service = new GuardianProfileService();
    (service as any).neynarService = {
      getUserByFid: async () => ({
        username: 'alice',
        pfp_url: 'https://example.com/alice.png',
      }),
    };

    const result = await service.resolveFromBrand({
      founderFid: 100,
      onChainFid: 200,
    });

    expect(result).toEqual({
      guardianFid: 100,
      guardianHandle: 'alice',
      guardianPfp: 'https://example.com/alice.png',
    });
  });

  it('falls back to onChainFid when founderFid is missing', async () => {
    const service = new GuardianProfileService();
    (service as any).neynarService = {
      getUserByFid: async () => ({
        username: 'bob',
        pfp_url: 'https://example.com/bob.png',
      }),
    };

    const result = await service.resolveFromBrand({
      onChainFid: 300,
    });

    expect(result.guardianFid).toBe(300);
    expect(result.guardianHandle).toBe('bob');
    expect(result.guardianPfp).toBe('https://example.com/bob.png');
  });

  it('returns null guardian fields when no fid exists', async () => {
    const service = new GuardianProfileService();

    const result = await service.resolveFromBrand({});

    expect(result).toEqual({
      guardianFid: null,
      guardianHandle: null,
      guardianPfp: null,
    });
  });

  it('keeps guardianFid and null profile fields when neynar fails', async () => {
    const service = new GuardianProfileService();
    (service as any).neynarService = {
      getUserByFid: async () => {
        throw new Error('boom');
      },
    };

    const result = await service.resolveFromBrand({
      founderFid: 777,
    });

    expect(result).toEqual({
      guardianFid: 777,
      guardianHandle: null,
      guardianPfp: null,
    });
  });
});
