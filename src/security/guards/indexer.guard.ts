import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Request } from 'express';
import { logger } from '../../main';
import { getConfig } from '../config';

/**
 * Indexer authentication guard for Ponder blockchain indexer requests.
 *
 * This guard validates incoming requests from the Ponder indexer by:
 * 1. Verifying the Authorization header contains the correct API key
 * 2. Checking for the required X-Indexer-Source header
 *
 * Expected request headers:
 * - Authorization: Bearer {INDEXER_API_KEY}
 * - X-Indexer-Source: ponder-stories-in-motion-v8
 */

@Injectable()
export class IndexerGuard implements CanActivate {
  private readonly EXPECTED_INDEXER_SOURCE = 'ponder-stories-in-motion-v8';

  /**
   * Extracts Bearer token from Authorization header.
   */
  private extractToken(req: Request): string | null {
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      return authHeader.substring(7);
    }
    return null;
  }

  /**
   * Validates indexer authentication.
   */
  async canActivate(context: ExecutionContext): Promise<boolean> {
    try {
      const req = context.switchToHttp().getRequest<Request>();
      const config = getConfig();

      // Check for indexer API key
      if (!process.env.INDEXER_API_KEY) {
        logger.error(
          '🔐 [INDEXER] INDEXER_API_KEY environment variable not set',
        );
        throw new UnauthorizedException(
          'Indexer authentication not configured',
        );
      }

      // Extract and verify Bearer token
      const token = this.extractToken(req);
      if (!token) {
        logger.warn('🔐 [INDEXER] Missing Authorization header');
        throw new UnauthorizedException('Authorization header required');
      }

      if (token !== process.env.INDEXER_API_KEY) {
        logger.warn('🔐 [INDEXER] Invalid API key provided');
        throw new UnauthorizedException('Invalid API key');
      }

      // Verify indexer source header
      const indexerSource = req.headers['x-indexer-source'];
      if (indexerSource !== this.EXPECTED_INDEXER_SOURCE) {
        logger.warn(
          `🔐 [INDEXER] Invalid indexer source: ${indexerSource}, expected: ${this.EXPECTED_INDEXER_SOURCE}`,
        );
        throw new UnauthorizedException('Invalid indexer source');
      }

      logger.log('✅ [INDEXER] Authentication successful');
      return true;
    } catch (error) {
      const message =
        error instanceof UnauthorizedException
          ? error.message
          : 'Indexer authentication failed';

      logger.error(`❌ [INDEXER] Authentication failed: ${message}`);
      throw new UnauthorizedException(message);
    }
  }
}
