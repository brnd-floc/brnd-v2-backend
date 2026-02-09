#!/usr/bin/env bun

/**
 * Brand ID Diagnostic & Repair Script
 *
 * Compares brands between the PostgreSQL indexer (source of truth) and MySQL
 * production database. Identifies:
 *   - Legacy brands that need onChainId populated (id matches, just missing onChainId)
 *   - ID mismatches (brand exists but MySQL id != on-chain id)
 *   - Truly missing brands (not in MySQL at all)
 *   - Data mismatches (handle/fid/wallet differ)
 *
 * Asks for confirmation before making any changes.
 *
 * Usage:
 *   bun run scripts/fix-brand-ids.ts              # Interactive diagnosis + repair
 *   bun run scripts/fix-brand-ids.ts --dry-run    # Diagnosis only, no changes
 *
 * Environment variables required:
 *   - INDEXER_DB_URL: PostgreSQL connection string
 *   - INDEXER_DB_SCHEMA: Schema name (default: public)
 *   - DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME: MySQL
 */

import { Client } from 'pg';
import * as mysql from 'mysql2/promise';
import * as readline from 'readline';

// ============================================================================
// Types
// ============================================================================

interface PgBrand {
  id: number;
  fid: number;
  wallet_address: string;
  handle: string;
  metadata_hash: string;
  total_brnd_awarded: string;
  available_brnd: string;
  created_at: string;
  block_number: string;
  transaction_hash: string;
}

interface MySqlBrand {
  id: number;
  name: string;
  onChainId: number | null;
  onChainHandle: string | null;
  onChainFid: number | null;
  onChainWalletAddress: string | null;
  metadataHash: string | null;
}

type IssueType = 'LEGACY_NEEDS_ONCHAIN_ID' | 'ID_MISMATCH' | 'TRULY_MISSING' | 'DATA_MISMATCH';

interface BrandIssue {
  type: IssueType;
  onChainId: number;
  pgHandle: string;
  pgFid: number;
  pgWalletAddress: string;
  mysqlId?: number;
  mysqlName?: string;
  mysqlOnChainId?: number | null;
  details: string;
}

// ============================================================================
// Helpers
// ============================================================================

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

function prompt(question: string): Promise<string> {
  return new Promise((resolve) => rl.question(question, resolve));
}

function namesMatch(mysqlName: string, pgHandle: string): boolean {
  // Normalize for comparison: lowercase, strip non-alphanumeric
  const normalize = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const a = normalize(mysqlName);
  const b = normalize(pgHandle);
  if (a === b) return true;
  // Handle cases like "anon_" / "anonxx" where names differ slightly
  // but are at the same ID slot (strong positional evidence they're the same brand)
  if (a.length >= 3 && b.length >= 3 && (a.startsWith(b) || b.startsWith(a))) return true;
  return false;
}

// ============================================================================
// Main
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');

  console.log('\n====================================================');
  console.log('  BRAND ID DIAGNOSTIC & REPAIR SCRIPT');
  console.log('====================================================');
  console.log(`  Mode: ${dryRun ? 'DRY RUN (read-only)' : 'INTERACTIVE (will ask before changes)'}`);
  console.log('  Source of truth: PostgreSQL indexer database');
  console.log('  Target: MySQL production database');
  console.log('====================================================\n');

  // Connect to databases
  const pgConnectionString = process.env.INDEXER_DB_URL;
  if (!pgConnectionString) {
    console.error('INDEXER_DB_URL environment variable is required');
    process.exit(1);
  }

  const schema = process.env.INDEXER_DB_SCHEMA || 'public';
  const pgClient = new Client({ connectionString: pgConnectionString });
  await pgClient.connect();
  console.log('Connected to PostgreSQL indexer database');

  const mysqlConfig = {
    host: process.env.DATABASE_HOST,
    port: parseInt(process.env.DATABASE_PORT || '3306', 10),
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_NAME,
  };

  if (!mysqlConfig.host || !mysqlConfig.user || !mysqlConfig.database) {
    console.error('MySQL environment variables required: DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME');
    process.exit(1);
  }

  const mysqlConn = await mysql.createConnection(mysqlConfig);
  console.log('Connected to MySQL production database\n');

  try {
    // ------------------------------------------------------------------
    // Fetch brands from both databases
    // ------------------------------------------------------------------
    console.log('Fetching brands from PostgreSQL indexer...');
    const pgResult = await pgClient.query(
      `SELECT id, fid, wallet_address, handle, metadata_hash,
              total_brnd_awarded, available_brnd, created_at,
              block_number, transaction_hash
       FROM "${schema}".brands ORDER BY id ASC`,
    );
    const pgBrands: PgBrand[] = pgResult.rows;
    console.log(`  Found ${pgBrands.length} brands in PostgreSQL\n`);

    console.log('Fetching brands from MySQL...');
    const [mysqlRows] = await mysqlConn.execute(
      `SELECT id, name, onChainId, onChainHandle, onChainFid,
              onChainWalletAddress, metadataHash
       FROM brands ORDER BY id ASC`,
    );
    const mysqlBrands = mysqlRows as MySqlBrand[];
    console.log(`  Found ${mysqlBrands.length} brands in MySQL\n`);

    // Build lookup maps
    const pgById = new Map<number, PgBrand>();
    for (const b of pgBrands) {
      pgById.set(b.id, b);
    }

    let mysqlByOnChainId = new Map<number, MySqlBrand>();
    let mysqlById = new Map<number, MySqlBrand>();
    const rebuildMysqlMaps = (brands: MySqlBrand[]) => {
      mysqlByOnChainId = new Map();
      mysqlById = new Map();
      for (const b of brands) {
        if (b.onChainId != null) {
          mysqlByOnChainId.set(b.onChainId, b);
        }
        mysqlById.set(b.id, b);
      }
    };
    rebuildMysqlMaps(mysqlBrands);

    // ------------------------------------------------------------------
    // Pre-diagnosis: Detect WRONG onChainId values
    // ------------------------------------------------------------------
    // A MySQL brand has a wrong onChainId if:
    //   - It has onChainId != id (assigned wrong on-chain ID by buggy sync)
    //   - A PG brand exists at the MySQL brand's `id` slot with a matching name
    //   - This means the MySQL brand IS the on-chain brand at its own `id`, but
    //     got the wrong onChainId from the sync script
    // ------------------------------------------------------------------
    interface WrongOnChainId {
      mysqlId: number;
      mysqlName: string;
      wrongOnChainId: number;
      correctOnChainId: number;
      pgHandle: string;
      pgFid: number;
      pgWalletAddress: string;
    }

    const wrongOnChainIds: WrongOnChainId[] = [];

    for (const mysqlBrand of mysqlBrands) {
      if (mysqlBrand.onChainId == null) continue;
      if (mysqlBrand.onChainId === mysqlBrand.id) continue;

      // This MySQL brand has onChainId != id. Check if PG brand at this MySQL id matches by name.
      const pgBrandAtSlot = pgById.get(mysqlBrand.id);
      if (pgBrandAtSlot && namesMatch(mysqlBrand.name, pgBrandAtSlot.handle)) {
        wrongOnChainIds.push({
          mysqlId: mysqlBrand.id,
          mysqlName: mysqlBrand.name,
          wrongOnChainId: mysqlBrand.onChainId,
          correctOnChainId: mysqlBrand.id,
          pgHandle: pgBrandAtSlot.handle,
          pgFid: pgBrandAtSlot.fid,
          pgWalletAddress: pgBrandAtSlot.wallet_address,
        });
      }
    }

    if (wrongOnChainIds.length > 0) {
      console.log('====================================================');
      console.log('  WRONG onChainId DETECTED (pre-diagnosis)');
      console.log('====================================================\n');
      console.log('  These MySQL brands have an incorrect onChainId that was');
      console.log('  set by the buggy sync script. Their MySQL id is correct,');
      console.log('  but onChainId points to a different on-chain brand.\n');

      for (const w of wrongOnChainIds) {
        console.log(`    MySQL id=${w.mysqlId} "${w.mysqlName}" has onChainId=${w.wrongOnChainId}`);
        console.log(`    PG brand at id=${w.correctOnChainId} is "${w.pgHandle}" (name match!)`);
        console.log(`    Fix: onChainId ${w.wrongOnChainId} -> ${w.correctOnChainId}\n`);
      }

      if (!dryRun) {
        const answer = await prompt('  Fix wrong onChainIds before continuing? (yes/no): ');
        if (answer.toLowerCase() === 'yes') {
          for (const w of wrongOnChainIds) {
            await mysqlConn.execute(
              `UPDATE brands
               SET onChainId = ?, onChainHandle = ?, onChainFid = ?, onChainWalletAddress = ?
               WHERE id = ?`,
              [w.correctOnChainId, w.pgHandle, w.pgFid, w.pgWalletAddress, w.mysqlId],
            );
            console.log(`    Fixed: id=${w.mysqlId} "${w.mysqlName}" onChainId: ${w.wrongOnChainId} -> ${w.correctOnChainId}`);

            // Update in-memory data so the main diagnosis is accurate
            const brand = mysqlBrands.find((b) => b.id === w.mysqlId);
            if (brand) {
              brand.onChainId = w.correctOnChainId;
              brand.onChainHandle = w.pgHandle;
              brand.onChainFid = w.pgFid;
              brand.onChainWalletAddress = w.pgWalletAddress;
            }
          }
          rebuildMysqlMaps(mysqlBrands);
          console.log('    Done. Continuing with main diagnosis...\n');
        } else {
          console.log('  Skipped. Main diagnosis may be inaccurate.\n');
        }
      } else {
        // In dry-run mode, still update in-memory data so the main diagnosis
        // shows what it would look like AFTER the pre-fix
        for (const w of wrongOnChainIds) {
          const brand = mysqlBrands.find((b) => b.id === w.mysqlId);
          if (brand) {
            brand.onChainId = w.correctOnChainId;
            brand.onChainHandle = w.pgHandle;
            brand.onChainFid = w.pgFid;
            brand.onChainWalletAddress = w.pgWalletAddress;
          }
        }
        rebuildMysqlMaps(mysqlBrands);
        console.log('  DRY RUN: These would be fixed before the main diagnosis.');
        console.log('  (Diagnosis below reflects the corrected state.)\n');
      }
    }

    // ------------------------------------------------------------------
    // Diagnose issues
    // ------------------------------------------------------------------
    console.log('====================================================');
    console.log('  DIAGNOSIS');
    console.log('====================================================\n');

    const issues: BrandIssue[] = [];

    for (const pgBrand of pgBrands) {
      const mysqlBrandByOnChain = mysqlByOnChainId.get(pgBrand.id);

      if (mysqlBrandByOnChain) {
        // Found by onChainId - check if MySQL id matches
        if (mysqlBrandByOnChain.id !== pgBrand.id) {
          issues.push({
            type: 'ID_MISMATCH',
            onChainId: pgBrand.id,
            pgHandle: pgBrand.handle,
            pgFid: pgBrand.fid,
            pgWalletAddress: pgBrand.wallet_address,
            mysqlId: mysqlBrandByOnChain.id,
            mysqlName: mysqlBrandByOnChain.name,
            mysqlOnChainId: mysqlBrandByOnChain.onChainId,
            details: `MySQL id=${mysqlBrandByOnChain.id} but should be ${pgBrand.id}`,
          });
        } else {
          // IDs match - check data
          const diffs: string[] = [];
          if (mysqlBrandByOnChain.onChainHandle !== pgBrand.handle)
            diffs.push(`handle: "${mysqlBrandByOnChain.onChainHandle}" -> "${pgBrand.handle}"`);
          if (mysqlBrandByOnChain.onChainFid !== pgBrand.fid)
            diffs.push(`fid: ${mysqlBrandByOnChain.onChainFid} -> ${pgBrand.fid}`);
          if (mysqlBrandByOnChain.onChainWalletAddress !== pgBrand.wallet_address)
            diffs.push(`wallet: "${mysqlBrandByOnChain.onChainWalletAddress}" -> "${pgBrand.wallet_address}"`);

          if (diffs.length > 0) {
            issues.push({
              type: 'DATA_MISMATCH',
              onChainId: pgBrand.id,
              pgHandle: pgBrand.handle,
              pgFid: pgBrand.fid,
              pgWalletAddress: pgBrand.wallet_address,
              mysqlId: mysqlBrandByOnChain.id,
              mysqlName: mysqlBrandByOnChain.name,
              details: diffs.join(', '),
            });
          }
        }
      } else {
        // No MySQL brand has onChainId == pgBrand.id
        // Check: is there a legacy brand at MySQL id == pgBrand.id with onChainId=null?
        const brandAtSlot = mysqlById.get(pgBrand.id);

        if (brandAtSlot && brandAtSlot.onChainId == null && namesMatch(brandAtSlot.name, pgBrand.handle)) {
          // Legacy brand: same ID slot, name matches, just needs onChainId set
          issues.push({
            type: 'LEGACY_NEEDS_ONCHAIN_ID',
            onChainId: pgBrand.id,
            pgHandle: pgBrand.handle,
            pgFid: pgBrand.fid,
            pgWalletAddress: pgBrand.wallet_address,
            mysqlId: brandAtSlot.id,
            mysqlName: brandAtSlot.name,
            details: `MySQL "${brandAtSlot.name}" at id=${brandAtSlot.id} needs onChainId=${pgBrand.id}`,
          });
        } else if (brandAtSlot && brandAtSlot.onChainId == null && !namesMatch(brandAtSlot.name, pgBrand.handle)) {
          // ID slot occupied by a different legacy brand - truly missing
          issues.push({
            type: 'TRULY_MISSING',
            onChainId: pgBrand.id,
            pgHandle: pgBrand.handle,
            pgFid: pgBrand.fid,
            pgWalletAddress: pgBrand.wallet_address,
            details: `No MySQL brand with onChainId=${pgBrand.id}. Slot id=${pgBrand.id} occupied by "${brandAtSlot.name}" (different brand, onChainId=null)`,
          });
        } else if (brandAtSlot && brandAtSlot.onChainId != null) {
          // Slot occupied by brand with a DIFFERENT onChainId
          issues.push({
            type: 'TRULY_MISSING',
            onChainId: pgBrand.id,
            pgHandle: pgBrand.handle,
            pgFid: pgBrand.fid,
            pgWalletAddress: pgBrand.wallet_address,
            details: `No MySQL brand with onChainId=${pgBrand.id}. Slot id=${pgBrand.id} occupied by "${brandAtSlot.name}" (onChainId=${brandAtSlot.onChainId})`,
          });
        } else {
          // Slot is empty - truly missing
          issues.push({
            type: 'TRULY_MISSING',
            onChainId: pgBrand.id,
            pgHandle: pgBrand.handle,
            pgFid: pgBrand.fid,
            pgWalletAddress: pgBrand.wallet_address,
            details: `No MySQL brand with onChainId=${pgBrand.id}. Slot id=${pgBrand.id} is empty.`,
          });
        }
      }
    }

    // ------------------------------------------------------------------
    // Display results
    // ------------------------------------------------------------------
    const legacyBrands = issues.filter((i) => i.type === 'LEGACY_NEEDS_ONCHAIN_ID');
    const idMismatches = issues.filter((i) => i.type === 'ID_MISMATCH');
    const trulyMissing = issues.filter((i) => i.type === 'TRULY_MISSING');
    const dataMismatches = issues.filter((i) => i.type === 'DATA_MISMATCH');

    if (issues.length === 0) {
      console.log('  All brands are correctly synced! No issues found.\n');
      return;
    }

    // --- Legacy brands needing onChainId ---
    if (legacyBrands.length > 0) {
      console.log(`  LEGACY BRANDS NEEDING onChainId (${legacyBrands.length}):`);
      console.log('  ' + '-'.repeat(80));
      console.log(`    These brands already exist at the correct MySQL id, they just`);
      console.log(`    need their onChainId, onChainHandle, onChainFid, and`);
      console.log(`    onChainWalletAddress fields populated.\n`);
      // Show first 5 and last 5
      const show = [...legacyBrands.slice(0, 5)];
      if (legacyBrands.length > 10) {
        show.push({ type: 'LEGACY_NEEDS_ONCHAIN_ID', onChainId: -1, pgHandle: '', pgFid: 0, pgWalletAddress: '', details: `... ${legacyBrands.length - 10} more ...` } as any);
      }
      if (legacyBrands.length > 5) {
        show.push(...legacyBrands.slice(-5));
      }
      for (const issue of show) {
        if (issue.onChainId === -1) {
          console.log(`    ${issue.details}`);
        } else {
          console.log(`    id=${issue.onChainId}: MySQL "${issue.mysqlName}" <- PG "${issue.pgHandle}" (fid=${issue.pgFid})`);
        }
      }
      console.log('');
    }

    // --- ID mismatches ---
    if (idMismatches.length > 0) {
      console.log(`  ID MISMATCHES (${idMismatches.length}):`);
      console.log('  ' + '-'.repeat(80));
      for (const issue of idMismatches) {
        console.log(`    On-chain ID: ${issue.onChainId}  (handle: "${issue.pgHandle}")`);
        console.log(`    MySQL ID:    ${issue.mysqlId}  (name: "${issue.mysqlName}", onChainId: ${issue.mysqlOnChainId})`);
        console.log(`    Fix:         Move MySQL id ${issue.mysqlId} -> ${issue.onChainId}`);
        console.log('');
      }
    }

    // --- Truly missing ---
    if (trulyMissing.length > 0) {
      console.log(`  TRULY MISSING BRANDS (${trulyMissing.length}):`);
      console.log('  ' + '-'.repeat(80));
      for (const issue of trulyMissing) {
        console.log(`    On-chain ID: ${issue.onChainId}  (handle: "${issue.pgHandle}", fid: ${issue.pgFid})`);
        console.log(`    Details:     ${issue.details}`);
        console.log('');
      }
    }

    // --- Data mismatches ---
    if (dataMismatches.length > 0) {
      console.log(`  DATA MISMATCHES (${dataMismatches.length}):`);
      console.log('  ' + '-'.repeat(80));
      for (const issue of dataMismatches) {
        console.log(`    On-chain ID: ${issue.onChainId}  (MySQL id: ${issue.mysqlId})`);
        console.log(`    Changes:     ${issue.details}`);
        console.log('');
      }
    }

    // Summary
    console.log('====================================================');
    console.log('  SUMMARY');
    console.log('====================================================');
    console.log(`  Legacy brands needing onChainId:  ${legacyBrands.length}`);
    console.log(`  ID mismatches:                    ${idMismatches.length}`);
    console.log(`  Truly missing brands:             ${trulyMissing.length}`);
    console.log(`  Data mismatches:                  ${dataMismatches.length}`);
    console.log('====================================================\n');

    if (dryRun) {
      console.log('  DRY RUN complete. No changes were made.\n');
      return;
    }

    // ------------------------------------------------------------------
    // Apply fixes (each with confirmation)
    // ------------------------------------------------------------------

    // 1. Fix ID mismatches (most critical - do first)
    if (idMismatches.length > 0) {
      console.log('====================================================');
      console.log('  FIX ID MISMATCHES');
      console.log('====================================================');
      console.log('  This will:');
      console.log('    1. Disable foreign key checks');
      console.log('    2. Move brands to temp IDs, then to correct on-chain IDs');
      console.log('    3. Re-enable foreign key checks');
      console.log('  NOTE: Vote references are NOT updated (they already use correct on-chain IDs).');
      console.log('');

      const answer = await prompt('  Proceed with fixing ID mismatches? (yes/no): ');
      if (answer.toLowerCase() === 'yes') {
        await fixIdMismatches(mysqlConn, idMismatches);
      } else {
        console.log('  Skipped.\n');
      }
    }

    // 2. Populate onChainId for legacy brands
    if (legacyBrands.length > 0) {
      console.log('====================================================');
      console.log('  POPULATE onChainId FOR LEGACY BRANDS');
      console.log('====================================================');
      console.log(`  This will set onChainId, onChainHandle, onChainFid, and`);
      console.log(`  onChainWalletAddress for ${legacyBrands.length} existing brands`);
      console.log(`  that already have the correct MySQL id.`);
      console.log('');

      const answer = await prompt('  Proceed? (yes/no): ');
      if (answer.toLowerCase() === 'yes') {
        await populateLegacyOnChainIds(mysqlConn, legacyBrands);
      } else {
        console.log('  Skipped.\n');
      }
    }

    // 3. Insert truly missing brands
    if (trulyMissing.length > 0) {
      console.log('====================================================');
      console.log('  INSERT TRULY MISSING BRANDS');
      console.log('====================================================');
      console.log(`  This will insert ${trulyMissing.length} brand(s) into MySQL`);
      console.log(`  with id = on-chain ID.`);
      console.log('');

      const answer = await prompt('  Proceed? (yes/no): ');
      if (answer.toLowerCase() === 'yes') {
        await insertMissingBrands(mysqlConn, pgClient, schema, trulyMissing);
      } else {
        console.log('  Skipped.\n');
      }
    }

    // 4. Fix data mismatches
    if (dataMismatches.length > 0) {
      console.log('====================================================');
      console.log('  FIX DATA MISMATCHES');
      console.log('====================================================');
      console.log(`  This will update on-chain fields for ${dataMismatches.length} brand(s).`);
      console.log('');

      const answer = await prompt('  Proceed? (yes/no): ');
      if (answer.toLowerCase() === 'yes') {
        await fixDataMismatches(mysqlConn, dataMismatches);
      } else {
        console.log('  Skipped.\n');
      }
    }

    // ------------------------------------------------------------------
    // Post-fix verification
    // ------------------------------------------------------------------
    console.log('\n====================================================');
    console.log('  POST-FIX VERIFICATION');
    console.log('====================================================\n');

    const [verifyMismatch] = await mysqlConn.execute(
      `SELECT id, name, onChainId FROM brands
       WHERE onChainId IS NOT NULL AND id != onChainId ORDER BY id ASC`,
    );
    const remaining = verifyMismatch as any[];

    const [verifyNull] = await mysqlConn.execute(
      `SELECT COUNT(*) as cnt FROM brands WHERE onChainId IS NULL`,
    );
    const nullCount = Number((verifyNull as any[])[0].cnt);

    if (remaining.length === 0 && nullCount === 0) {
      console.log('  All brands have id == onChainId. Everything looks good!\n');
    } else {
      if (remaining.length > 0) {
        console.log(`  ${remaining.length} brands still have id != onChainId:`);
        for (const b of remaining.slice(0, 10)) {
          console.log(`    id=${b.id}, onChainId=${b.onChainId}, name="${b.name}"`);
        }
        if (remaining.length > 10) console.log(`    ... and ${remaining.length - 10} more`);
        console.log('');
      }
      if (nullCount > 0) {
        console.log(`  ${nullCount} brands still have onChainId=NULL\n`);
      }
    }
  } catch (error) {
    console.error('\nFatal error:', error);
    process.exit(1);
  } finally {
    await pgClient.end();
    await mysqlConn.end();
    rl.close();
    console.log('Database connections closed.\n');
  }
}

// ============================================================================
// Fix Functions
// ============================================================================

/**
 * Fix ID mismatches using a safe two-phase temp-ID approach to avoid
 * cascading reference corruption.
 *
 * Phase 1: Move all affected brands to temporary negative IDs
 * Phase 2: Move brands from temp IDs to correct on-chain IDs
 *
 * Vote references are NOT updated — they already use correct on-chain IDs.
 */
async function fixIdMismatches(
  mysqlConn: mysql.Connection,
  issues: BrandIssue[],
): Promise<void> {
  console.log('\n  Fixing ID mismatches...\n');

  await mysqlConn.execute('SET FOREIGN_KEY_CHECKS = 0');
  console.log('    Foreign key checks disabled.');

  try {
    // Phase 1: Move brands to temp negative IDs
    console.log('\n    Phase 1: Moving brands to temporary IDs...');

    // First, check if target slots are occupied by non-mismatch brands and move them
    for (const issue of issues) {
      const targetId = issue.onChainId;
      const [existing] = await mysqlConn.execute(
        'SELECT id, name, onChainId FROM brands WHERE id = ?',
        [targetId],
      );
      const rows = existing as any[];
      if (rows.length > 0) {
        const isPartOfMismatchSet = issues.some((i) => i.mysqlId === targetId);
        if (!isPartOfMismatchSet) {
          // This brand occupies a slot we need. Move it to a temp ID.
          const conflictTempId = -(targetId + 100000);
          console.log(`      Conflict: id=${targetId} "${rows[0].name}" -> temp ${conflictTempId}`);
          await mysqlConn.execute('UPDATE brands SET id = ? WHERE id = ?', [conflictTempId, targetId]);
        }
      }
    }

    // Now move each mismatched brand to its temp ID
    for (const issue of issues) {
      const oldId = issue.mysqlId!;
      const tempId = -oldId;

      // Move brand to temp
      await mysqlConn.execute('UPDATE brands SET id = ? WHERE id = ?', [tempId, oldId]);
      console.log(`      Brand "${issue.pgHandle}": id ${oldId} -> ${tempId} (temp)`);
    }

    // Phase 2: Move from temp IDs to correct on-chain IDs
    console.log('\n    Phase 2: Moving from temporary to correct IDs...');
    for (const issue of issues) {
      const tempId = -issue.mysqlId!;
      const correctId = issue.onChainId;

      await mysqlConn.execute(
        'UPDATE brands SET id = ?, onChainId = ? WHERE id = ?',
        [correctId, correctId, tempId],
      );
      console.log(`      Brand "${issue.pgHandle}": ${tempId} (temp) -> ${correctId} (correct)`);
    }

    console.log('\n    ID mismatches fixed successfully!');
  } catch (error) {
    console.error('\n    ERROR during ID mismatch fix:', error);
    console.error('    Some changes may have been partially applied. Review manually.');
  } finally {
    await mysqlConn.execute('SET FOREIGN_KEY_CHECKS = 1');
    console.log('    Foreign key checks re-enabled.\n');
  }
}


/**
 * Populate onChainId + on-chain fields for legacy brands that already have
 * the correct MySQL id but are missing on-chain metadata.
 */
async function populateLegacyOnChainIds(
  mysqlConn: mysql.Connection,
  issues: BrandIssue[],
): Promise<void> {
  console.log(`\n  Updating ${issues.length} legacy brands...\n`);

  let updated = 0;
  for (const issue of issues) {
    try {
      await mysqlConn.execute(
        `UPDATE brands
         SET onChainId = ?, onChainHandle = ?, onChainFid = ?, onChainWalletAddress = ?
         WHERE id = ? AND onChainId IS NULL`,
        [issue.onChainId, issue.pgHandle, issue.pgFid, issue.pgWalletAddress, issue.mysqlId],
      );
      updated++;
    } catch (error) {
      console.error(`    ERROR updating brand id=${issue.mysqlId}:`, error);
    }
  }

  console.log(`    Updated ${updated}/${issues.length} legacy brands.\n`);
}

/**
 * Insert brands that are completely missing from MySQL.
 */
async function insertMissingBrands(
  mysqlConn: mysql.Connection,
  pgClient: Client,
  schema: string,
  issues: BrandIssue[],
): Promise<void> {
  console.log(`\n  Inserting ${issues.length} missing brand(s)...\n`);

  // Get default category
  const [categoryRows] = await mysqlConn.execute(
    `SELECT id FROM categories WHERE name = 'General' LIMIT 1`,
  );
  let defaultCategoryId: number;
  if ((categoryRows as any[]).length === 0) {
    const [insertResult] = await mysqlConn.execute(
      `INSERT INTO categories (name) VALUES ('General')`,
    );
    defaultCategoryId = (insertResult as any).insertId;
  } else {
    defaultCategoryId = (categoryRows as any[])[0].id;
  }

  await mysqlConn.execute('SET FOREIGN_KEY_CHECKS = 0');

  try {
    for (const issue of issues) {
      const onChainId = issue.onChainId;

      // Check if slot is occupied
      const [existing] = await mysqlConn.execute('SELECT id, name FROM brands WHERE id = ?', [onChainId]);
      if ((existing as any[]).length > 0) {
        const occ = (existing as any[])[0];
        console.log(`    SKIP id=${onChainId}: slot occupied by "${occ.name}". Fix ID mismatches first.`);
        continue;
      }

      // Fetch from PG
      const pgResult = await pgClient.query(
        `SELECT id, fid, wallet_address, handle, metadata_hash,
                total_brnd_awarded, available_brnd, created_at
         FROM "${schema}".brands WHERE id = $1`,
        [onChainId],
      );
      if (pgResult.rows.length === 0) continue;
      const pg = pgResult.rows[0];

      // IPFS metadata
      let metadata: any = {};
      let metadataHash = pg.metadata_hash || '';
      if (metadataHash.startsWith('ipfs://')) metadataHash = metadataHash.slice(7);
      else if (metadataHash.startsWith('/ipfs/')) metadataHash = metadataHash.slice(6);

      if (metadataHash) {
        try {
          const resp = await fetch(`https://ipfs.io/ipfs/${metadataHash}`);
          if (resp.ok) metadata = await resp.json();
        } catch { /* fallback to handle */ }
      }

      const name = metadata.name || pg.handle;
      const profile = metadata.profile || '';
      const channel = metadata.channel || (!profile ? `/${pg.handle}` : '');

      await mysqlConn.execute(
        `INSERT INTO brands (
          id, onChainId, onChainHandle, onChainFid, onChainWalletAddress, onChainCreatedAt,
          metadataHash, name, url, warpcastUrl, description, imageUrl, profile, channel,
          queryType, followerCount, categoryId, score, stateScore, scoreDay, stateScoreDay,
          scoreWeek, stateScoreWeek, scoreMonth, stateScoreMonth, ranking, rankingWeek,
          rankingMonth, bonusPoints, banned, currentRanking, totalBrndAwarded, availableBrnd,
          createdAt, updatedAt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())`,
        [
          onChainId, onChainId, pg.handle, pg.fid, pg.wallet_address,
          new Date(Number(pg.created_at) * 1000), metadataHash,
          name, metadata.url || '', metadata.warpcastUrl || metadata.url || '',
          metadata.description || '', metadata.imageUrl || '', profile, channel,
          metadata.queryType ?? 0, metadata.followerCount || 0, defaultCategoryId,
          0, 0, 0, 0, 0, 0, 0, 0,    // scores
          '0', 0, 0,                   // rankings
          0, 0, 0,                     // bonusPoints, banned, currentRanking
          pg.total_brnd_awarded?.toString() || '0',
          pg.available_brnd?.toString() || '0',
        ],
      );

      console.log(`    Inserted: "${name}" (id=${onChainId}, handle="${pg.handle}")`);
    }

    console.log('\n    Done inserting missing brands.');
  } catch (error) {
    console.error('\n    ERROR:', error);
  } finally {
    await mysqlConn.execute('SET FOREIGN_KEY_CHECKS = 1');
    console.log('    Foreign key checks re-enabled.\n');
  }
}

/**
 * Fix data mismatches (on-chain fields differ from PG source of truth).
 */
async function fixDataMismatches(
  mysqlConn: mysql.Connection,
  issues: BrandIssue[],
): Promise<void> {
  console.log('\n  Fixing data mismatches...\n');

  for (const issue of issues) {
    try {
      await mysqlConn.execute(
        `UPDATE brands SET onChainHandle = ?, onChainFid = ?, onChainWalletAddress = ? WHERE id = ?`,
        [issue.pgHandle, issue.pgFid, issue.pgWalletAddress, issue.mysqlId],
      );
      console.log(`    Updated id=${issue.onChainId}: ${issue.details}`);
    } catch (error) {
      console.error(`    ERROR on id=${issue.onChainId}:`, error);
    }
  }

  console.log('\n    Data mismatches fixed.\n');
}

// ============================================================================
main();
