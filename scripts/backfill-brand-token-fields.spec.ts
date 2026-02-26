import { describe, it, expect } from '@jest/globals';
import * as fs from 'fs';
import * as path from 'path';

describe('backfill-brand-token-fields script', () => {
  const scriptPath = path.join(
    process.cwd(),
    'scripts',
    'backfill-brand-token-fields.ts',
  );

  it('includes tickerTokenId candidate condition in query', () => {
    const content = fs.readFileSync(scriptPath, 'utf8');

    expect(content).toContain('brand.tickerTokenId IS NULL');
    expect(content).toContain("brand.tickerTokenId = ''");
  });

  it('prints field-level summary metrics', () => {
    const content = fs.readFileSync(scriptPath, 'utf8');

    expect(content).toContain('contractAddressUpdated');
    expect(content).toContain('tickerUpdated');
    expect(content).toContain('tickerTokenIdUpdated');
  });
});
