// Dependencies
import { Injectable, OnModuleInit } from '@nestjs/common';

// Utils
import { logger } from '../../../main';

/**
 * Authentication service for Farcaster QuickAuth integration.
 *
 * This service provides JWT token verification using Farcaster's QuickAuth system.
 * QuickAuth eliminates the need for custom session management by providing
 * cryptographically signed JWTs that can be verified server-side without
 * database lookups or custom token generation.
 *
 * The service initializes the Farcaster QuickAuth client on module startup
 * and provides methods for token verification. User creation and management
 * is handled separately in the UserService to maintain separation of concerns.
 */
@Injectable()
export class AuthService implements OnModuleInit {
  private farcasterClient: any;

  constructor() {}

  /**
   * Initializes the Farcaster QuickAuth client on module startup.
   * Uses dynamic import to load the ES module since @farcaster/quick-auth
   * is not available as a CommonJS module.
   */
  async onModuleInit() {
    try {
      const importFn = new Function('specifier', 'return import(specifier)');
      const module = await importFn('@farcaster/quick-auth');
      const { createClient } = module;
      this.farcasterClient = createClient();
    } catch (error) {
      logger.error('Failed to initialize Farcaster QuickAuth client:', error);
      throw new Error('QuickAuth initialization failed: ' + error.message);
    }
  }

  /**
   * Ensures the Farcaster QuickAuth client is available for use.
   * Handles lazy initialization if the client was not properly set up during module init.
   */
  private async ensureFarcasterClient() {
    if (!this.farcasterClient) {
      await this.onModuleInit();
    }
  }

  /**
   * Verifies a QuickAuth JWT token against Farcaster's verification service.
   *
   * This method validates that:
   * - The JWT signature is valid and from Farcaster's auth server
   * - The token hasn't expired
   * - The token was issued for the correct domain
   *
   * @param token - JWT token received from the Farcaster miniapp frontend
   * @returns Promise resolving to the verified JWT payload containing user FID and address
   * @throws Error if token verification fails
   */
  async verifyQuickAuthToken(token: string) {
    await this.ensureFarcasterClient();

    try {
      const domain = 'brnd.land';
      const payload = await this.farcasterClient.verifyJwt({ token, domain });

      if (!payload || !payload.sub) {
        throw new Error('Invalid token payload: missing user FID');
      }

      return payload;
    } catch (error) {
      try {
        const domain = 'miniapp.anky.app';
        const payload = await this.farcasterClient.verifyJwt({ token, domain });

        if (!payload || !payload.sub) {
          throw new Error('Invalid token payload: missing user FID');
        }

        return payload;
      } catch (error) {
        logger.error('QuickAuth token verification failed:', error.message);
        throw new Error('Token verification failed again: ' + error.message);
      }
    }
  }
}
