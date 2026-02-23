import { Injectable, Logger } from '@nestjs/common';
import NeynarService from '../../../utils/neynar';

export type GuardianProfile = {
  guardianFid: number | null;
  guardianHandle: string | null;
  guardianPfp: string | null;
};

type GuardianSourceBrand = {
  founderFid?: number | null;
  onChainFid?: number | null;
};

@Injectable()
export class GuardianProfileService {
  private readonly logger = new Logger(GuardianProfileService.name);
  private readonly neynarService = new NeynarService();
  private readonly requestTimeoutMs = Number(
    process.env.NEYNAR_GUARDIAN_TIMEOUT_MS ?? 2500,
  );

  private resolveGuardianFid(brand: GuardianSourceBrand): number | null {
    return brand?.founderFid ?? brand?.onChainFid ?? null;
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    let timeoutId: NodeJS.Timeout | null = null;
    try {
      return await Promise.race([
        promise,
        new Promise<T>((_, reject) => {
          timeoutId = setTimeout(
            () => reject(new Error(`Timeout after ${timeoutMs}ms`)),
            timeoutMs,
          );
        }),
      ]);
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  async resolveFromBrand(brand: GuardianSourceBrand): Promise<GuardianProfile> {
    const guardianFid = this.resolveGuardianFid(brand);

    if (!guardianFid) {
      return {
        guardianFid: null,
        guardianHandle: null,
        guardianPfp: null,
      };
    }

    try {
      const user = await this.withTimeout(
        this.neynarService.getUserByFid(guardianFid),
        this.requestTimeoutMs,
      );

      const guardianHandle = user?.username ?? null;
      const guardianPfp = user?.pfp_url ?? null;

      if (!guardianHandle || !guardianPfp) {
        this.logger.warn(
          `Guardian profile incomplete for FID ${guardianFid}: handle=${Boolean(
            guardianHandle,
          )} pfp=${Boolean(guardianPfp)}`,
        );
      }

      return {
        guardianFid,
        guardianHandle,
        guardianPfp,
      };
    } catch (error) {
      this.logger.warn(
        `Failed to hydrate guardian profile for FID ${guardianFid}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      return {
        guardianFid,
        guardianHandle: null,
        guardianPfp: null,
      };
    }
  }
}
