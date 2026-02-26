import { ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { WebhookApiKeyGuard } from './webhook-api-key.guard';
import { afterEach, describe, expect, it } from '@jest/globals';

describe('WebhookApiKeyGuard', () => {
  const makeContext = (headers: Record<string, string>): ExecutionContext =>
    ({
      switchToHttp: () => ({
        getRequest: () => ({ headers }),
      }),
    }) as ExecutionContext;

  const originalKey = process.env.WEBHOOK_API_KEY;

  afterEach(() => {
    process.env.WEBHOOK_API_KEY = originalKey;
  });

  it('allows request when x-webhook-api-key matches', () => {
    process.env.WEBHOOK_API_KEY = 'expected-secret';
    const guard = new WebhookApiKeyGuard();

    expect(
      guard.canActivate(
        makeContext({ 'x-webhook-api-key': 'expected-secret' }),
      ),
    ).toBe(true);
  });

  it('rejects request when key is missing or invalid', () => {
    process.env.WEBHOOK_API_KEY = 'expected-secret';
    const guard = new WebhookApiKeyGuard();

    expect(() => guard.canActivate(makeContext({}))).toThrow(
      UnauthorizedException,
    );
    expect(() =>
      guard.canActivate(makeContext({ 'x-webhook-api-key': 'wrong' })),
    ).toThrow(UnauthorizedException);
  });
});
