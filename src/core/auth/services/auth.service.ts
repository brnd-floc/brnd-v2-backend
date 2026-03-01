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
  private quickAuthDomains: string[] = [];
  private quickAuthLegacyDomains: string[] = [];

  private static readonly DEFAULT_QUICKAUTH_DOMAINS_FINAL = [
    'brnd.land',
    'www.brnd.land',
    'frame.brnd.land',
  ];

  private static readonly DEFAULT_QUICKAUTH_DOMAINS_LEGACY = [
    'miniapp.anky.app',
    'brndland.com',
    'miniapp.brndland.com',
    'api.brnd.land',
  ];

  constructor() {}

  private warnLegacyDomainDrift(domain: string): void {
    logger.warn(
      `event=config_drift_detected component=auth.quickauth domainOrUrl=${domain} env=production action=remove_legacy_from_QUICKAUTH_ALLOWED_DOMAINS`,
    );
  }

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
      const domainConfig = this.loadQuickAuthDomainConfig();
      this.quickAuthDomains = domainConfig.domains;
      this.quickAuthLegacyDomains = domainConfig.legacyDomains;
    } catch (error) {
      logger.error('Failed to initialize Farcaster QuickAuth client:', error);
      throw new Error('QuickAuth initialization failed: ' + error.message);
    }
  }

  private parseDomainCsv(value?: string): string[] {
    if (!value) {
      return [];
    }

    return value
      .split(',')
      .map((domain) => domain.trim())
      .filter(Boolean);
  }

  private loadQuickAuthDomainConfig(): {
    domains: string[];
    legacyDomains: string[];
  } {
    const configured = process.env.QUICKAUTH_ALLOWED_DOMAINS;
    const configuredLegacy = process.env.QUICKAUTH_ALLOWED_DOMAINS_LEGACY;
    const isProduction = process.env.NODE_ENV === 'production';

    if (configured !== undefined) {
      const parsed = this.parseDomainCsv(configured);

      if (parsed.length === 0) {
        throw new Error(
          'QUICKAUTH_ALLOWED_DOMAINS is empty. Provide at least one allowed domain.',
        );
      }

      if (isProduction) {
        const legacySet = new Set(AuthService.DEFAULT_QUICKAUTH_DOMAINS_LEGACY);
        for (const domain of parsed) {
          if (legacySet.has(domain)) {
            this.warnLegacyDomainDrift(domain);
          }
        }
      }

      return {
        domains: parsed,
        legacyDomains: this.parseDomainCsv(configuredLegacy),
      };
    }

    const finalDomains = [...AuthService.DEFAULT_QUICKAUTH_DOMAINS_FINAL];
    const legacyDomains =
      configuredLegacy !== undefined
        ? this.parseDomainCsv(configuredLegacy)
        : [...AuthService.DEFAULT_QUICKAUTH_DOMAINS_LEGACY];

    const composed = isProduction
      ? finalDomains
      : [...finalDomains, ...legacyDomains];
    const uniqueDomains = Array.from(new Set(composed));

    if (uniqueDomains.length === 0) {
      throw new Error(
        'QuickAuth allowed domains resolved to an empty list. Configure QUICKAUTH_ALLOWED_DOMAINS.',
      );
    }

    return {
      domains: uniqueDomains,
      legacyDomains,
    };
  }

  private loadQuickAuthDomains(): string[] {
    return this.loadQuickAuthDomainConfig().domains;
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
    const domainsToTry =
      this.quickAuthDomains.length > 0
        ? this.quickAuthDomains
        : this.loadQuickAuthDomains();
    const legacySet = new Set(this.quickAuthLegacyDomains);
    let lastError: Error | null = null;

    for (let i = 0; i < domainsToTry.length; i++) {
      const domain = domainsToTry[i];
      try {
        const payload = await this.farcasterClient.verifyJwt({ token, domain });

        if (!payload || !payload.sub) {
          throw new Error('Invalid token payload: missing user FID');
        }

        if (i > 0) {
          const legacySuffix = legacySet.has(domain) ? ' [legacy]' : '';
          logger.warn(
            `QuickAuth token verified using fallback domain "${domain}"${legacySuffix} (position ${i + 1}/${domainsToTry.length})`,
          );
        }

        return payload;
      } catch (error) {
        lastError = error as Error;
      }
    }

    logger.error(
      `QuickAuth token verification failed for all configured domains (${domainsToTry.join(', ')}): ${lastError?.message || 'unknown error'}`,
    );
    throw new Error(
      `Token verification failed: ${lastError?.message || 'unknown error'}`,
    );
  }
}
