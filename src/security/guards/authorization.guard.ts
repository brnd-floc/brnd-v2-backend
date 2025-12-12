// Dependencies
import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Request, Response } from 'express';

// Services
import { AuthService } from '../../core/auth/services';

// Utils
import { logger } from '../../main';
import { getConfig } from '../config';

/**
 * Authorization guard for Farcaster QuickAuth JWT tokens.
 *
 * This guard validates incoming requests by verifying QuickAuth JWT tokens
 * provided by Farcaster miniapp clients. It supports multiple token sources:
 *
 * 1. Authorization header with Bearer token (primary method for miniapps)
 * 2. Authorization cookie (fallback for web-based access)
 *
 * The guard uses Farcaster's QuickAuth verification service to validate tokens
 * cryptographically without requiring database lookups. Verified token payload
 * is attached to the request object for use by route handlers.
 *
 * Design rationale:
 * - No custom JWT signing/verification (delegates to Farcaster's secure service)
 * - Stateless authentication (no server-side session storage required)
 * - Compatible with Farcaster's miniapp security model
 */

// QuickAuth JWT payload structure as defined by Farcaster
export interface QuickAuthPayload {
  sub: number; // Farcaster ID (FID) of the authenticated user
  address: string; // Ethereum address used for authentication
  iss: string; // Issuer: Farcaster's QuickAuth server URL
  aud: string; // Audience: the domain this token is valid for
  exp: number; // Expiration timestamp (Unix time)
  iat: number; // Issued at timestamp (Unix time)
}

@Injectable()
export class AuthorizationGuard implements CanActivate {
  constructor(private readonly authService: AuthService) {}

  /**
   * Extracts JWT token from request headers or cookies.
   * Prioritizes Authorization header (standard for API clients) over cookies.
   *
   * @param req - Incoming HTTP request
   * @returns JWT token string or null if not found
   */
  private extractToken(req: Request): string | null {
    // Primary: Authorization header with Bearer token (miniapp standard)
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      return authHeader.substring(7);
    }

    // Fallback: Authorization cookie (web compatibility)
    const cookieToken = req.cookies['Authorization'];
    if (cookieToken) {
      return cookieToken;
    }

    return null;
  }

  /**
   * Validates request authorization using QuickAuth token verification.
   *
   * This method:
   * 1. Extracts JWT token from request
   * 2. Verifies token with Farcaster's QuickAuth service
   * 3. Attaches verified user payload to request object
   * 4. Allows request to proceed to route handler
   *
   * @param context - NestJS execution context
   * @returns Promise<boolean> - true if authorization succeeds
   * @throws UnauthorizedException if token is missing or invalid
   */
  async canActivate(context: ExecutionContext): Promise<boolean> {
    try {
      const req = context
        .switchToHttp()
        .getRequest<Request & { user: QuickAuthPayload }>();

      const token = this.extractToken(req);
      if (!token) {
        throw new UnauthorizedException('Authentication token required');
      }

      // Verify token against Farcaster's QuickAuth service

      const payload = await this.authService.verifyQuickAuthToken(token);

      // Attach verified user data to request for route handlers
      req.user = payload;

      return true;
    } catch (error) {
      // Clear any invalid cookies to prevent repeated failed requests
      const res: Response = context.switchToHttp().getResponse();
      res.clearCookie('Authorization');

      const message =
        error instanceof UnauthorizedException
          ? error.message
          : 'Token verification failed';

      throw new UnauthorizedException(message);
    }
  }
}
