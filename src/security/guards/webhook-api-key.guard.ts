import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Request } from 'express';

@Injectable()
export class WebhookApiKeyGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    // Hook for future hardening: apply IP-based rate limiting at this guard boundary.
    const req = context.switchToHttp().getRequest<Request>();
    const expectedKey = process.env.WEBHOOK_API_KEY;

    if (!expectedKey) {
      throw new UnauthorizedException('Webhook authentication not configured');
    }

    const providedKey = req.headers['x-webhook-api-key'];
    const key = Array.isArray(providedKey) ? providedKey[0] : providedKey;

    if (!key || key !== expectedKey) {
      throw new UnauthorizedException('Invalid webhook API key');
    }

    return true;
  }
}
