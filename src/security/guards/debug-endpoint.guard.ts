import {
  CanActivate,
  ExecutionContext,
  Injectable,
  NotFoundException,
} from '@nestjs/common';

@Injectable()
export class DebugEndpointGuard implements CanActivate {
  canActivate(_context: ExecutionContext): boolean {
    const isProduction = process.env.NODE_ENV === 'production';
    const debugEnabled = process.env.ENABLE_DEBUG_ENDPOINTS === 'true';

    if (isProduction && !debugEnabled) {
      throw new NotFoundException();
    }

    return true;
  }
}
