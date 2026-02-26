import { describe, expect, it } from '@jest/globals';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('BlockchainController guards metadata', () => {
  const source = readFileSync(
    resolve(__dirname, 'blockchain.controller.ts'),
    'utf8',
  );

  it('protects Farcaster cast webhook with WebhookApiKeyGuard decorator', () => {
    expect(source).toMatch(
      /@Post\('\/webhooks\/farcaster\/cast-created'\)\s*@UseGuards\(WebhookApiKeyGuard\)/m,
    );
  });

  it('protects debug NFT endpoints with AdminGuard and DebugEndpointGuard decorators', () => {
    expect(source).toMatch(
      /@Get\('\/podium\/test-nft-image'\)\s*@UseGuards\(AdminGuard, DebugEndpointGuard\)/m,
    );
    expect(source).toMatch(
      /@Get\('\/podium\/test-nft-image\/preview'\)\s*@UseGuards\(AdminGuard, DebugEndpointGuard\)/m,
    );
  });
});
