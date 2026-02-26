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
import { UserRoleEnum } from '../../models/User';
import { QuickAuthPayload } from './authorization.guard';

/**
 * Admin authorization guard that extends the base AuthorizationGuard.
 * Ensures the user is authenticated AND has admin role.
 */
@Injectable()
export class AdminGuard extends AuthorizationGuard {
  private readonly fallbackAdminFids = [5431, 16098, 8109, 39278];

  constructor(authService: AuthService) {
    super(authService); // Pass AuthService to parent constructor
  }

  private getConfiguredAdminFids(): number[] {
    const raw = process.env.ADMIN_FIDS;
    if (!raw) {
      return this.fallbackAdminFids;
    }

    const parsed = raw
      .split(',')
      .map((fid) => Number(fid.trim()))
      .filter((fid) => Number.isFinite(fid));

    return parsed.length > 0 ? parsed : this.fallbackAdminFids;
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
      .getRequest<Request & { user: QuickAuthPayload & { role?: UserRoleEnum } }>();
    const user = req.user;
    const adminFids = this.getConfiguredAdminFids();
    const hasAdminFid = adminFids.includes(Number(user.sub));
    const hasAdminRole = user.role === UserRoleEnum.ADMIN;

    if (!hasAdminFid && !hasAdminRole) {
      throw new ForbiddenException(
        'You do not have permission to access this resource',
      );
    }

    return true;
  }
}
