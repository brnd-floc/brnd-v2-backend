import { describe, expect, it, jest } from '@jest/globals';
import { GUARDS_METADATA } from '@nestjs/common/constants';
import { WebhookApiKeyGuard } from '../../security/guards';

jest.mock('src/main', () => ({ logger: { log: jest.fn(), warn: jest.fn(), error: jest.fn() } }), {
  virtual: true,
});
jest.mock('../../main', () => ({
  logger: { log: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));

const { NotificationController } = require('./notification.controller');

describe('NotificationController guards metadata', () => {
  it('protects farcaster webhook with WebhookApiKeyGuard', () => {
    const guards = Reflect.getMetadata(
      GUARDS_METADATA,
      NotificationController.prototype.farcasterWebhook,
    ) as unknown[];

    expect(guards).toBeDefined();
    expect(guards).toContain(WebhookApiKeyGuard);
  });
});
