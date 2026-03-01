import { describe, it, expect } from '@jest/globals';
import domains from './config';

describe('security domains config', () => {
  it('keeps production cors origins aligned with brnd.land cutover', () => {
    expect(domains.PRO).toEqual([
      'https://brnd.land',
      'https://www.brnd.land',
      'https://frame.brnd.land',
      'https://api.brnd.land',
    ]);
  });

  it('does not allow deprecated legacy app origins in production', () => {
    expect(domains.PRO).not.toContain('https://miniapp.anky.app');
    expect(domains.PRO).not.toContain('https://miniapp.brndland.com');
  });
});
