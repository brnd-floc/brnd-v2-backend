import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

type BrandDetailResponse = {
  brand?: {
    id?: number;
    name?: string;
    onChainFid?: number | null;
    guardianFid?: number | null;
    guardianHandle?: string | null;
    guardianPfp?: string | null;
    ticker?: string | null;
    contractAddress?: string | null;
    tickerTokenId?: string | null;
    tokenTicker?: string | null;
    tokenContractAddress?: string | null;
    category?: {
      id?: number;
      name?: string | null;
    } | null;
  };
};

type EnhancedBrandShape = {
  id?: number;
  guardianFid?: number | null;
  guardianHandle?: string | null;
  guardianPfp?: string | null;
  ticker?: string | null;
  contractAddress?: string | null;
  tickerTokenId?: string | null;
  tokenTicker?: string | null;
  tokenContractAddress?: string | null;
};

type EnhancedBrandResponse = {
  data?: EnhancedBrandShape;
} & EnhancedBrandShape;

type CheckResult = {
  checks: string[];
  warnings: string[];
  failures: string[];
};

type Options = {
  apiBase: string;
  brandIds: number[];
  healthPath: string;
  skipHealth: boolean;
  strictCategory: boolean;
  outPath?: string;
};

function parseArgs(args: string[]): Options {
  const arg = (name: string) => args.find((v) => v.startsWith(`${name}=`));
  const has = (name: string) => args.includes(name);

  const apiBase = (arg('--api-base')?.split('=')[1] || process.env.CUTOVER_API_BASE || 'https://api.brnd.land').replace(/\/$/, '');
  const brandIdsRaw = arg('--brand-ids')?.split('=')[1] || process.env.CUTOVER_BRAND_IDS || '431,428,1';
  const brandIds = brandIdsRaw
    .split(',')
    .map((v) => Number(v.trim()))
    .filter((v) => Number.isFinite(v) && v > 0);

  const healthPath = arg('--health-path')?.split('=')[1] || process.env.CUTOVER_HEALTH_PATH || '/embeds/health';
  const outPath = arg('--out')?.split('=')[1] || process.env.CUTOVER_BASELINE_OUT;

  return {
    apiBase,
    brandIds,
    healthPath,
    skipHealth: has('--skip-health'),
    strictCategory: has('--strict-category'),
    outPath,
  };
}

async function fetchJson(url: string): Promise<any> {
  const response = await fetch(url, { headers: { Accept: 'application/json' } });
  const text = await response.text();

  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${url} :: ${text.slice(0, 200)}`);
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`Invalid JSON from ${url}`);
  }
}

function isNumericName(value: string | null | undefined): boolean {
  return typeof value === 'string' && /^\d+$/.test(value.trim());
}

function pushTypeFailure(result: CheckResult, condition: boolean, message: string): void {
  if (!condition) result.failures.push(message);
  else result.checks.push(message);
}

function validateBrandContract(
  id: number,
  detail: BrandDetailResponse,
  enhanced: EnhancedBrandResponse,
  strictCategory: boolean,
): CheckResult {
  const result: CheckResult = { checks: [], warnings: [], failures: [] };
  const brand = detail.brand;
  const enhancedData =
    enhanced && typeof enhanced === 'object'
      ? (enhanced.data && typeof enhanced.data === 'object'
          ? enhanced.data
          : enhanced) as EnhancedBrandShape
      : undefined;

  pushTypeFailure(result, !!brand, `brand ${id}: detail payload contains brand`);
  if (!brand) return result;

  pushTypeFailure(
    result,
    typeof brand.guardianFid === 'number' || brand.guardianFid === null,
    `brand ${id}: guardianFid is number|null`,
  );
  pushTypeFailure(
    result,
    typeof brand.guardianHandle === 'string' || brand.guardianHandle === null,
    `brand ${id}: guardianHandle is string|null`,
  );
  pushTypeFailure(
    result,
    typeof brand.guardianPfp === 'string' || brand.guardianPfp === null,
    `brand ${id}: guardianPfp is string|null`,
  );

  pushTypeFailure(
    result,
    brand.tokenTicker === (brand.ticker ?? null),
    `brand ${id}: tokenTicker alias matches ticker`,
  );
  pushTypeFailure(
    result,
    brand.tokenContractAddress === (brand.contractAddress ?? null),
    `brand ${id}: tokenContractAddress alias matches contractAddress`,
  );

  if (brand.guardianFid == null && brand.onChainFid == null) {
    result.warnings.push(`brand ${id}: no guardianFid and no onChainFid (guardian card will not render)`);
  }

  const categoryName = brand.category?.name;
  if (isNumericName(categoryName)) {
    const msg = `brand ${id}: category.name is numeric (${categoryName})`;
    if (strictCategory) result.failures.push(msg);
    else result.warnings.push(msg);
  }

  if (enhancedData) {
    pushTypeFailure(
      result,
      enhancedData.tokenTicker === (enhancedData.ticker ?? null),
      `brand ${id}: enhanced tokenTicker alias matches ticker`,
    );
    pushTypeFailure(
      result,
      enhancedData.tokenContractAddress === (enhancedData.contractAddress ?? null),
      `brand ${id}: enhanced tokenContractAddress alias matches contractAddress`,
    );
  } else {
    result.failures.push(`brand ${id}: enhanced payload missing data field`);
  }

  return result;
}

async function run(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));

  if (options.brandIds.length === 0) {
    throw new Error('No valid brand ids. Use --brand-ids=431,428,1');
  }

  console.log('🚀 Cutover API verification');
  console.log(`API Base: ${options.apiBase}`);
  console.log(`Brand IDs: ${options.brandIds.join(', ')}`);
  console.log(`Health path: ${options.healthPath} ${options.skipHealth ? '(skipped)' : ''}`);

  const startedAt = new Date().toISOString();
  const baseline: Record<string, any> = {};
  const failures: string[] = [];
  const warnings: string[] = [];

  if (!options.skipHealth) {
    const healthUrl = `${options.apiBase}${options.healthPath}`;
    try {
      const health = await fetchJson(healthUrl);
      baseline.health = health;
      console.log(`✅ health check ok: ${healthUrl}`);
    } catch (error) {
      failures.push(`health check failed for ${healthUrl}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  for (const id of options.brandIds) {
    const detailUrl = `${options.apiBase}/brand-service/brand/${id}`;
    const enhancedUrl = `${options.apiBase}/brand-service/brand/${id}/enhanced`;

    try {
      const [detail, enhanced] = await Promise.all([
        fetchJson(detailUrl),
        fetchJson(enhancedUrl),
      ]);

      baseline[`brand_${id}`] = {
        detail,
        enhanced,
      };

      const result = validateBrandContract(
        id,
        detail as BrandDetailResponse,
        enhanced as EnhancedBrandResponse,
        options.strictCategory,
      );

      for (const check of result.checks) {
        console.log(`✅ ${check}`);
      }
      for (const warning of result.warnings) {
        warnings.push(warning);
        console.log(`⚠️ ${warning}`);
      }
      for (const failure of result.failures) {
        failures.push(failure);
        console.log(`❌ ${failure}`);
      }
    } catch (error) {
      failures.push(`brand ${id}: fetch failed: ${error instanceof Error ? error.message : String(error)}`);
      console.log(`❌ brand ${id}: request failure`);
    }
  }

  const report = {
    startedAt,
    finishedAt: new Date().toISOString(),
    apiBase: options.apiBase,
    brandIds: options.brandIds,
    warnings,
    failures,
    baseline,
  };

  if (options.outPath) {
    mkdirSync(dirname(options.outPath), { recursive: true });
    writeFileSync(options.outPath, JSON.stringify(report, null, 2));
    console.log(`📝 baseline written to ${options.outPath}`);
  }

  console.log(`\nSummary: ${failures.length} failure(s), ${warnings.length} warning(s)`);

  if (failures.length > 0) {
    process.exitCode = 1;
    return;
  }
}

run().catch((error) => {
  console.error('❌ cutover verification failed:', error);
  process.exitCode = 1;
});
