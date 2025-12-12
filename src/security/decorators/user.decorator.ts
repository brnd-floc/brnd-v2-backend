// Dependencies
import {
  createParamDecorator,
  ExecutionContext,
  UnauthorizedException,
} from '@nestjs/common';
import { CurrentUser as User } from '../../models/User';
import { QuickAuthPayload } from '../guards';

/**
 * Custom decorator to extract the current user from the request within the NestJS execution context.
 * This decorator ensures the user object is correctly typed as `User` or returns `null` if no user is found.
 *
 * @param {unknown} data - An optional parameter not used in this decorator, but required for custom decorators.
 * @param {ExecutionContext} ctx - The execution context from which the HTTP request is extracted.
 * @returns {User | null} - The extracted user object from the request, typed as `User` or `null` if not present.
 */
export const Session = createParamDecorator(
  (data: unknown, ctx: ExecutionContext): QuickAuthPayload => {
    const request = ctx.switchToHttp().getRequest();

    console.log('🔍 [Session Decorator] User object:', request.user);
    console.log('🔍 [Session Decorator] User FID (sub):', request.user?.sub);

    if (!request.user) {
      throw new Error('No authenticated user found in request');
    }

    return request.user as QuickAuthPayload;
  },
);

export type { User };
