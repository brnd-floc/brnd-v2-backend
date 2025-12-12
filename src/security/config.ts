import { Logger } from '@nestjs/common';

const logger = new Logger('APISystem');

/**
 * Configuration object for the application environment.
 * @property {boolean} isProduction - Determines if the environment is production based on the ENV variable.
 * @property {Object} runtime - Contains runtime configuration.
 * @property {number|string} runtime.port - The port the application runs on, defaults to 8080 if not specified.
 * @property {Object} db - Contains database connection configuration.
 * @property {string} db.name - The name of the database from the DATABASE_NAME environment variable.
 * @property {string} db.host - The database host, defaults to an empty string if not specified.
 * @property {number} db.port - The database port, parsed from the DATABASE_PORT environment variable, defaults to 5432 if not specified.
 * @property {string} db.username - The database username from the DATABASE_USER environment variable.
 * @property {string} db.password - The database password from the DATABASE_PASSWORD environment variable.
 */

export const getConfig = () => ({
  identifier: process.env.IDENTIFIER || 'BRND API',
  version: process.env.VERSION || '1.0',
  // Fix: Change ENV check to NODE_ENV (standard)
  isProduction: process.env.NODE_ENV === 'production',
  runtime: {
    host: process.env.HOST || '',
    // Fix: Use PORT for production (DigitalOcean App Platform requirement)
    port:
      process.env.PORT || (process.env.NODE_ENV === 'production' ? 3000 : 8080),
  },
  session: {
    key: process.env.SESSION_KEY || 'x059bb@_358b',
    domain: process.env.SESSION_DOMAIN || '127.0.0.1',
  },
  db: {
    name: process.env.DATABASE_NAME,
    host: process.env.DATABASE_HOST || '',
    // Fix: MySQL default port should be 3306, not 5432 (PostgreSQL)
    port: parseInt(process.env.DATABASE_PORT || '', 0) || 3306,
    username: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    // Add SSL configuration for DigitalOcean managed DB
    requireSSL:
      process.env.DATABASE_SSL === 'true' ||
      process.env.NODE_ENV === 'production',
  },
  neynar: {
    apiKey: process.env.NEYNAR_API_KEY,
    signerUuid: process.env.NEYNAR_SIGNER_UUID,
  },
  blockchain: {
    baseRpcUrl: process.env.BASE_RPC_URL,
    contractAddress: process.env.CONTRACT_ADDRESS,
    backendPrivateKey: process.env.PRIVATE_KEY,
    brndTokenAddress:
      process.env.BRND_TOKEN_ADDRESS ||
      '0x41Ed0311640A5e489A90940b1c33433501a21B07',
    tellerVaultAddress:
      process.env.TELLER_VAULT_ADDRESS ||
      '0x19d1872d8328b23a219e11d3d6eeee1954a88f88',
    airdropContractAddress: process.env.AIRDROP_CONTRACT_ADDRESS,
  },
  collectiblesIndexer: {
    apiRoute: process.env.COLLECTIBLES_INDEXER_API_ROUTE,
    apiKey: process.env.COLLECTIBLES_INDEXER_API_KEY,
  },
  notifications: {
    enabled: false,
    baseUrl: process.env.NOTIFICATION_BASE_URL || 'https://www.brnd.land',
    miniappUrl: process.env.MINIAPP_URL || 'https://www.brnd.land',
    dailyReminderHour: parseInt(process.env.DAILY_REMINDER_HOUR || '10', 10),
    eveningReminderHour: parseInt(
      process.env.EVENING_REMINDER_HOUR || '20',
      10,
    ),
    maxRetries: parseInt(process.env.NOTIFICATION_MAX_RETRIES || '3', 10),
    rateLimitPerMinute: parseInt(
      process.env.NOTIFICATION_RATE_LIMIT || '100',
      10,
    ),
  },
  tools: {},
  startup: () => {
    logger.log(`
      ╔═════════════════════════════════════════════════════════════════════════════╗
      ║                                                                             ║
      ║    ██████╗ ██████╗ ███╗   ██╗██████╗      █████╗ ██████╗ ██╗                ║
      ║    ██╔══██╗██╔══██╗████╗  ██║██╔══██╗    ██╔══██╗██╔══██╗██║                ║
      ║    ██████╔╝██████╔╝██╔██╗ ██║██║  ██║    ███████║██████╔╝██║                ║
      ║    ██╔══██╗██╔══██╗██║╚██╗██║██║  ██║    ██╔══██║██╔═══╝ ██║                ║
      ║    ██████╔╝██║  ██║██║ ╚████║██████╔╝    ██║  ██║██║     ██║                ║
      ║    ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝     ╚═╝  ╚═╝╚═╝     ╚═╝                ║
      ║                                                                             ║
      ║                          🏆 FARCASTER MINIAPP BACKEND 🏆                    ║
      ║   Version ${getConfig().version} - Evolution Edition                        ║
      ║                                                                             ║
      ╠═════════════════════════════════════════════════════════════════════════════╣
      ║                                                                             ║
      ║    🎨 Original Creators:                                                    ║
      ║       • German Debonis Schneck (german.schneck@gmail.com)                   ║
      ║       • Jorge Gomes Durán (jgomes79@hotmail.es)                             ║
      ║                                                                             ║
      ║    ⚡ Evolution Engineer:                                                    ║
      ║       • Jorge Pablo Franetovic (jpfraneto@gmail.com)                        ║
      ║         "Inheriting greatness, evolving excellence"                          ║
      ║                                                                              ║
      ╠══════════════════════════════════════════════════════════════════════════════╣
      ║                                                                              ║
      ║  🚀 SYSTEM STATUS:                                                           ║
      ║                                                                              ║
      ║    ✅ QuickAuth Integration    ✅ TypeORM Database                           ║
      ║    ✅ NestJS Architecture     ✅ JWT Verification                            ║
      ║    ✅ Modern Authentication   ✅ Modular Design                              ║
      ║    ✅ Production Ready        ✅ Farcaster Miniapp                           ║
      ║    ${process.env.NODE_ENV === 'production' ? '🌐 PRODUCTION MODE' : '🔧 DEVELOPMENT MODE'}              ║
      ║                                                                              ║
      ║  🌐 Server listening on: http://localhost:${getConfig().runtime.port}                             ║
      ║  📡 Database: Connected & Synchronized                                       ║
      ║  🔐 Auth: Farcaster QuickAuth Enabled                                       ║
      ║  🗄️  SSL: ${getConfig().db.requireSSL ? 'Enabled' : 'Disabled'}                                      ║
      ║                                                                              ║
      ╠══════════════════════════════════════════════════════════════════════════════╣
      ║                                                                              ║
      ║  ⚖️  COPYRIGHT & LICENSE NOTICE                                              ║
      ║                                                                              ║
      ║     This software is protected under copyright law. Unauthorized            ║
      ║     reproduction, distribution, or modification is strictly prohibited       ║
      ║     and may result in legal action. All rights reserved.                    ║
      ║                                                                              ║
      ║     © ${new Date().getFullYear()} - Licensed under proprietary terms                              ║
      ║                                                                              ║
      ╠══════════════════════════════════════════════════════════════════════════════╣
      ║                                                                              ║
      ║  🎯 READY TO SERVE THE FARCASTER ECOSYSTEM                                   ║
      ║     Building the future of decentralized social interactions                 ║
      ║                                                                              ║
      ╚══════════════════════════════════════════════════════════════════════════════╝
      
      🔗 API Documentation: ${process.env.NODE_ENV === 'production' ? 'Disabled in production' : 'Available in development mode'}
      📊 Health Check: All systems operational and ready for production
      
    `);
  },
});

/**
 * Configuration options for CSRF protection middleware.
 * @property {Object} cookie - The configuration for the cookie to be set by CSRF middleware.
 * @property {string} cookie.key - The name of the cookie.
 * @property {boolean} cookie.sameSite - Strictly set to the same site for CSRF protection.
 * @property {boolean} cookie.httpOnly - Ensures the cookie is sent only over HTTP(S), not accessible through JavaScript.
 * @property {boolean} cookie.secure - Ensures the cookie is sent over HTTPS.
 */
export const csurfConfigOptions = {
  cookie: {
    key: '_csrf',
    sameSite: true,
    httpOnly: true,
    secure: true,
  },
};

// Types
type Domains = Record<'LOCAL' | 'STAGING' | 'PRO', string[]>;

/**
 * Domains configuration for different environments.
 * LOCAL: Domains for local development.
 * DEV: Domains for the development environment.
 * PRO: Domains for the production environment.
 */
const domains: Domains = {
  LOCAL: [
    'http://127.0.0.1:5173',
    'http://localhost:5173',
    'https://miniapp.anky.app',
    'https://brnd.lat',
    'https://rebrnd.lat',
    'https://brnd-dashboard.orbiter.website',
  ],
  STAGING: [],
  PRO: [
    'http://127.0.0.1:5173',
    'http://localhost:5173',
    'https://www.brnd.land',
    'https://www.brnd.land',
    'https://rebrnd.lat',
    'https://frame.brnd.land',
    'https://brnd-frame.azurewebsites.net',
    'https://miniapp.anky.app',
    'https://brnd.lat',
    'https://brnd-dashboard.orbiter.website',
    '*',
  ],
};

export default domains;
