/**
 * LICENSE AND COPYRIGHT NOTICE
 *
 * This software and its source code are protected under legal copyright laws.
 * It is strictly prohibited to alter, duplicate, or reproduce any part of this
 * software without the express written consent of the creator. Unauthorized use,
 * duplication, or reproduction of this software may result in legal action and
 * will be prosecuted to the fullest extent under the law. All rights reserved
 * to the creator.
 *
 * @author Jorge Pablo Franetovich <jpfraneto@gmail.com>
 * @link https://www.farcaster.xyz/jpfraneto.eth
 */

// Dependencies
import { NestFactory } from '@nestjs/core';
import { SwaggerModule } from '@nestjs/swagger';
import { Logger, ValidationPipe } from '@nestjs/common';

// Security
import helmet from 'helmet';
import domains, { getConfig } from './security/config';

// Environment
import * as dotenv from 'dotenv';
dotenv.config({ path: '.env' });

// Modules
import { AppModule } from './app.module';

// Docs
import { swaggerOptions } from './doc';

export const logger = new Logger('APIGateway');

async function bootstrap() {
  try {
    const app = await NestFactory.create(AppModule);

    app.useGlobalPipes(
      new ValidationPipe({
        transform: true,
        whitelist: true,
        forbidNonWhitelisted: false,
      }),
    );

    // CSRF middleware is intentionally disabled for API paths because
    // authentication is header-based (Bearer token), not cookie-based.

    if (!getConfig().isProduction) {
      const document = SwaggerModule.createDocument(app, swaggerOptions);
      SwaggerModule.setup('doc', app, document);
    } else {
      app.use(helmet());
    }

    app.enableCors({
      origin: getConfig().isProduction
        ? domains.PRO
        : [...domains.LOCAL, ...domains.STAGING],

      credentials: true,
    });

    await app.listen(getConfig().runtime.port);

    getConfig().startup();
  } catch (e) {
    logger.error(e);
  }
}
void bootstrap();
