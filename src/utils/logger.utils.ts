/**
 * Production-safe logging utilities
 * Reduces log pollution in production environments
 */

const isProduction = process.env.NODE_ENV === 'production';
const isDevelopment = process.env.NODE_ENV === 'development';

/**
 * Log only in development environment
 */
export const devLog = (logger: any, message: string, ...args: any[]) => {
  if (isDevelopment) {
    logger.log(message, ...args);
  }
};

/**
 * Log debug information only in non-production environments
 */
export const debugLog = (logger: any, message: string, ...args: any[]) => {
  if (!isProduction) {
    logger.debug(message, ...args);
  }
};

/**
 * Log verbose information only in development
 */
export const verboseLog = (logger: any, message: string, ...args: any[]) => {
  if (isDevelopment) {
    logger.verbose(message, ...args);
  }
};

/**
 * Always log warnings and errors
 */
export const warnLog = (logger: any, message: string, ...args: any[]) => {
  logger.warn(message, ...args);
};

export const errorLog = (logger: any, message: string, ...args: any[]) => {
  logger.error(message, ...args);
};

/**
 * Clean log message for production (removes emojis)
 */
export const cleanMessage = (message: string): string => {
  if (isProduction) {
    // Remove emojis and reduce verbosity for production
    return message.replace(/[\u{1F600}-\u{1F64F}]|[\u{1F300}-\u{1F5FF}]|[\u{1F680}-\u{1F6FF}]|[\u{1F1E0}-\u{1F1FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/gu, '').trim();
  }
  return message;
};

/**
 * Log critical operations that should always be logged but cleanly
 */
export const criticalLog = (logger: any, message: string, ...args: any[]) => {
  logger.log(cleanMessage(message), ...args);
};

/**
 * Conditional logging based on log level
 */
export const conditionalLog = (
  logger: any, 
  level: 'debug' | 'verbose' | 'log' | 'warn' | 'error',
  message: string, 
  ...args: any[]
) => {
  switch (level) {
    case 'debug':
      debugLog(logger, message, ...args);
      break;
    case 'verbose':
      verboseLog(logger, message, ...args);
      break;
    case 'log':
      devLog(logger, message, ...args);
      break;
    case 'warn':
      warnLog(logger, message, ...args);
      break;
    case 'error':
      errorLog(logger, message, ...args);
      break;
  }
};