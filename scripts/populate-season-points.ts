#!/usr/bin/env bun

/**
 * Populate Season Points Script
 *
 * Sets totalS1Points from old S1 database and totalS2Points from votes.
 * Does NOT modify the existing points column.
 *
 * Usage:
 *   bun run scripts/populate-season-points.ts --dry-run   # Preview changes
 *   bun run scripts/populate-season-points.ts             # Apply changes
 */

import * as mysql from 'mysql2/promise';

interface UserUpdate {
  fid: number;
  username: string;
  currentPoints: number;
  oldS1Points: number;
  s2Points: number;
  newTotalPoints: number;
}

async function main() {
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run');

  console.log('\n' + '='.repeat(60));
  console.log('         POPULATE SEASON POINTS');
  console.log('='.repeat(60));
  console.log(`Mode: ${isDryRun ? 'DRY RUN' : 'LIVE'}`);
  console.log('Sets totalS1Points and totalS2Points (does not modify points)');
  console.log('='.repeat(60));

  // Connect to current database
  console.log('\n🔌 Connecting to current database...');
  const currentDb = await mysql.createConnection({
    host: process.env.DATABASE_HOST,
    port: parseInt(process.env.DATABASE_PORT || '3306', 10),
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_NAME,
  });
  console.log('✅ Connected to current database');

  // Connect to old S1 production database
  console.log('🔌 Connecting to old S1 production database...');
  const oldDb = await mysql.createConnection({
    host: process.env.PROD_DATABASE_HOST,
    port: parseInt(process.env.PROD_DATABASE_PORT || '3306', 10),
    user: process.env.PROD_DATABASE_USER,
    password: process.env.PROD_DATABASE_PASSWORD,
    database: process.env.PROD_DATABASE_NAME,
  });
  console.log('✅ Connected to old S1 production database');

  try {
    // Get S1 points from old database
    console.log('\n📊 Fetching S1 points from old database...');
    const [oldS1Rows] = await oldDb.execute(`
      SELECT fid, username, points FROM users WHERE fid IS NOT NULL
    `);
    const oldS1Map = new Map<number, number>();
    for (const row of oldS1Rows as any[]) {
      oldS1Map.set(row.fid, Number(row.points));
    }
    console.log(`   Found ${oldS1Map.size} users in old S1 database`);

    // Get S2 calculated points and current data from current database
    console.log('📊 Fetching current data and calculating S2 points...');
    const [currentRows] = await currentDb.execute(`
      SELECT
        u.fid,
        u.username,
        u.points as currentPoints,
        COALESCE(SUM(CASE WHEN v.season = 2 THEN v.pointsEarned ELSE 0 END), 0) as s2Points
      FROM users u
      LEFT JOIN user_brand_votes v ON v.userId = u.id
      WHERE u.fid IS NOT NULL
      GROUP BY u.id
    `);
    console.log(`   Found ${(currentRows as any[]).length} users in current database`);

    // Prepare updates
    const updates: UserUpdate[] = [];

    for (const row of currentRows as any[]) {
      const fid = row.fid;
      const currentPoints = Number(row.currentPoints);
      const s2Points = Number(row.s2Points);
      const oldS1Points = oldS1Map.get(fid) || 0;
      const newTotalPoints = oldS1Points + s2Points;

      updates.push({
        fid,
        username: row.username || `user_${fid}`,
        currentPoints,
        oldS1Points,
        s2Points,
        newTotalPoints,
      });
    }

    // Summary
    const usersWithS1 = updates.filter(u => u.oldS1Points > 0).length;
    const usersWithS2 = updates.filter(u => u.s2Points > 0).length;
    const totalS1 = updates.reduce((sum, u) => sum + u.oldS1Points, 0);
    const totalS2 = updates.reduce((sum, u) => sum + u.s2Points, 0);

    console.log('\n' + '='.repeat(60));
    console.log('SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total users:              ${updates.length}`);
    console.log(`Users with S1 points:     ${usersWithS1}`);
    console.log(`Users with S2 points:     ${usersWithS2}`);
    console.log('');
    console.log(`Total S1 points:          ${totalS1.toLocaleString()}`);
    console.log(`Total S2 points:          ${totalS2.toLocaleString()}`);

    // Show sample
    console.log('\n' + '='.repeat(60));
    console.log('SAMPLE (top 10 by total points)');
    console.log('='.repeat(60));
    console.log('FID       | Username         | S1 Pts  | S2 Pts  | Total');
    console.log('-'.repeat(60));

    const sorted = [...updates].sort((a, b) => b.newTotalPoints - a.newTotalPoints);
    for (const u of sorted.slice(0, 10)) {
      console.log(
        `${u.fid.toString().padEnd(9)} | ${u.username.slice(0, 16).padEnd(16)} | ${u.oldS1Points.toString().padStart(7)} | ${u.s2Points.toString().padStart(7)} | ${u.newTotalPoints.toString().padStart(7)}`
      );
    }

    // Apply changes
    if (!isDryRun) {
      console.log('\n📝 Applying changes...');

      const startTime = Date.now();

      // Bulk update S1 points using CASE statement
      console.log('   Updating S1 points...');
      const s1Updates = updates.filter(u => u.oldS1Points > 0);
      if (s1Updates.length > 0) {
        // Process in chunks of 500 to avoid query size limits
        const CHUNK_SIZE = 500;
        for (let i = 0; i < s1Updates.length; i += CHUNK_SIZE) {
          const chunk = s1Updates.slice(i, i + CHUNK_SIZE);
          const caseStatements = chunk.map(u => `WHEN ${u.fid} THEN ${u.oldS1Points}`).join(' ');
          const fids = chunk.map(u => u.fid).join(',');

          await currentDb.execute(`
            UPDATE users
            SET totalS1Points = CASE fid ${caseStatements} END
            WHERE fid IN (${fids})
          `);

          process.stdout.write(`\r   S1: ${Math.min(i + CHUNK_SIZE, s1Updates.length)}/${s1Updates.length}...`);
        }
        console.log(`\n   ✅ Updated ${s1Updates.length} users with S1 points`);
      }

      // Bulk update S2 points using CASE statement
      console.log('   Updating S2 points...');
      const s2Updates = updates.filter(u => u.s2Points > 0);
      if (s2Updates.length > 0) {
        const CHUNK_SIZE = 500;
        for (let i = 0; i < s2Updates.length; i += CHUNK_SIZE) {
          const chunk = s2Updates.slice(i, i + CHUNK_SIZE);
          const caseStatements = chunk.map(u => `WHEN ${u.fid} THEN ${u.s2Points}`).join(' ');
          const fids = chunk.map(u => u.fid).join(',');

          await currentDb.execute(`
            UPDATE users
            SET totalS2Points = CASE fid ${caseStatements} END
            WHERE fid IN (${fids})
          `);

          process.stdout.write(`\r   S2: ${Math.min(i + CHUNK_SIZE, s2Updates.length)}/${s2Updates.length}...`);
        }
        console.log(`\n   ✅ Updated ${s2Updates.length} users with S2 points`);
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(`\n⏱️ Completed in ${duration}s`);
    } else {
      console.log('\n⚠️ DRY RUN - No changes made');
      console.log('   Run without --dry-run to apply changes');
    }

    console.log('\n' + '='.repeat(60));

  } finally {
    await currentDb.end();
    await oldDb.end();
  }
}

main().catch(console.error);
