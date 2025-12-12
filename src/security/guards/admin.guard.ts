// Dependencies
import {
  UnauthorizedException,
  ForbiddenException,
  Injectable,
  ExecutionContext,
} from '@nestjs/common';
import { Request } from 'express';

// Guards
import { AuthorizationGuard } from './authorization.guard';

// Services
import { AuthService } from '../../core/auth/services';

// Types
import { CurrentUser, UserRoleEnum } from '../../models/User';

// Utils
import { logger } from '../../main';

/**
 * Admin authorization guard that extends the base AuthorizationGuard.
 * Ensures the user is authenticated AND has admin role.
 */
@Injectable()
export class AdminGuard extends AuthorizationGuard {
  constructor(authService: AuthService) {
    super(authService); // Pass AuthService to parent constructor
  }

  async canActivate(context: ExecutionContext): Promise<boolean> {
    // First check if user is authenticated (parent guard)
    const canActivate = await super.canActivate(context);

    if (!canActivate) {
      throw new UnauthorizedException('You are not authenticated');
    }

    // Then check if user has admin role
    const req = context
      .switchToHttp()
      .getRequest<Request & { user: CurrentUser }>();
    const user = req.user;

    if (user.role !== UserRoleEnum.ADMIN) {
      throw new ForbiddenException(
        'You do not have permission to access this resource',
      );
    }

    return true;
  }
}
