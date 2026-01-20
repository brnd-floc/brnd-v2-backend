#!/usr/bin/env bun

/**
 * Backfill Season Flag Script
 *
 * Sets the `season` column for all votes based on their date.
 *
 * Season 1: votes before Dec 13, 2025 06:50:00 UTC
 * Season 2: votes on or after Dec 13, 2025 06:50:00 UTC
 *
 * This script uses bulk UPDATE queries so it's very fast.
 *
 * Usage:
 *   bun run scripts/backfill-season.ts
 *   bun run scripts/backfill-season.ts --dry-run
 */

import * as mysql from 'mysql2/promise';

// Season 2 started at Dec 13, 2025 06:50:00 UTC
const SEASON_2_START_DATE = '2025-12-13 06:50:00';

async function main() {
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run');

  console.log('\n' + '='.repeat(50));
  console.log('         SEASON FLAG BACKFILL');
  console.log('='.repeat(50));
  console.log(`Mode: ${isDryRun ? 'DRY RUN (no changes)' : 'LIVE'}`);
  console.log(`Season 2 start: ${SEASON_2_START_DATE} UTC`);
  console.log('='.repeat(50));

  // Connect to MySQL
  console.log('\n🔌 Connecting to MySQL...');

  const mysqlConfig = {
    host: process.env.DATABASE_HOST,
    port: parseInt(process.env.DATABASE_PORT || '3306', 10),
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_NAME,
  };

  if (!mysqlConfig.host || !mysqlConfig.user || !mysqlConfig.database) {
    console.error(
      '❌ MySQL environment variables required: DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME',
    );
    process.exit(1);
  }

  const conn = await mysql.createConnection(mysqlConfig);
  console.log('✅ Connected to MySQL');

  try {
    // Count votes by season first
    console.log('\n📊 Counting votes...');

    const [season1Count] = await conn.execute(`
      SELECT COUNT(*) as cnt FROM user_brand_votes WHERE date < ?
    `, [SEASON_2_START_DATE]);

    const [season2Count] = await conn.execute(`
      SELECT COUNT(*) as cnt FROM user_brand_votes WHERE date >= ?
    `, [SEASON_2_START_DATE]);

    const [nullSeasonCount] = await conn.execute(`
      SELECT COUNT(*) as cnt FROM user_brand_votes WHERE season IS NULL
    `);

    const s1Count = (season1Count as any[])[0].cnt;
    const s2Count = (season2Count as any[])[0].cnt;
    const nullCount = (nullSeasonCount as any[])[0].cnt;

    console.log(`   Season 1 votes: ${s1Count}`);
    console.log(`   Season 2 votes: ${s2Count}`);
    console.log(`   Votes with NULL season: ${nullCount}`);

    if (isDryRun) {
      console.log('\n⚠️ DRY RUN - No changes made');
      console.log(`   Would set ${s1Count} votes to season = 1`);
      console.log(`   Would set ${s2Count} votes to season = 2`);
    } else {
      console.log('\n📝 Updating season flags...');

      const startTime = Date.now();

      // Bulk update Season 1
      const [result1] = await conn.execute(`
        UPDATE user_brand_votes SET season = 1 WHERE date < ?
      `, [SEASON_2_START_DATE]);
      const updated1 = (result1 as any).affectedRows;
      console.log(`   ✅ Set ${updated1} votes to season = 1`);

      // Bulk update Season 2
      const [result2] = await conn.execute(`
        UPDATE user_brand_votes SET season = 2 WHERE date >= ?
      `, [SEASON_2_START_DATE]);
      const updated2 = (result2 as any).affectedRows;
      console.log(`   ✅ Set ${updated2} votes to season = 2`);

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(`\n⏱️ Completed in ${duration}s`);
    }

    // Verify
    console.log('\n📊 Verification...');
    const [verify] = await conn.execute(`
      SELECT season, COUNT(*) as cnt
      FROM user_brand_votes
      GROUP BY season
      ORDER BY season
    `);
    console.log('   Current distribution:');
    for (const row of verify as any[]) {
      const seasonLabel = row.season === null ? 'NULL' : `Season ${row.season}`;
      console.log(`   - ${seasonLabel}: ${row.cnt} votes`);
    }

    console.log('\n✅ Done!');
  } catch (error) {
    console.error('\n❌ Error:', error);
    process.exit(1);
  } finally {
    await conn.end();
  }
}

main();
