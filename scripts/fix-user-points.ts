#!/usr/bin/env bun

/**
 * Fix User Points Script
 *
 * Updates user points using:
 * - Season 1: Points from old production database (source of truth)
 * - Season 2: Sum of pointsEarned from votes
 *
 * Usage:
 *   bun run scripts/fix-user-points.ts --dry-run   # Preview changes
 *   bun run scripts/fix-user-points.ts             # Apply changes
 *   bun run scripts/fix-user-points.ts --analyze   # Deep analysis of S1 differences
 */

import * as mysql from 'mysql2/promise';

interface UserPoints {
  fid: number;
  username: string;
  currentDbPoints: number;
  oldS1Points: number;
  s2CalculatedPoints: number;
  newTotalPoints: number;
  difference: number;
}

async function main() {
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run');
  const analyzeMode = args.includes('--analyze');

  console.log('\n' + '='.repeat(60));
  console.log('         FIX USER POINTS');
  console.log('='.repeat(60));
  console.log(`Mode: ${isDryRun ? 'DRY RUN' : analyzeMode ? 'ANALYZE' : 'LIVE'}`);
  console.log('Formula: User Points = Old S1 Points + S2 Calculated Points');
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
    if (analyzeMode) {
      await analyzeS1Differences(currentDb, oldDb);
      return;
    }

    // Get S1 points from old database
    console.log('\n📊 Fetching S1 points from old database...');
    const [oldS1Rows] = await oldDb.execute(`
      SELECT fid, username, points FROM users WHERE fid IS NOT NULL
    `);
    const oldS1Map = new Map<number, { points: number; username: string }>();
    for (const row of oldS1Rows as any[]) {
      oldS1Map.set(row.fid, { points: Number(row.points), username: row.username });
    }
    console.log(`   Found ${oldS1Map.size} users in old S1 database`);

    // Get S2 calculated points and current points from current database
    console.log('📊 Fetching current data...');
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

    // Calculate new points for each user
    const updates: UserPoints[] = [];
    let onlyS2Users = 0;

    for (const row of currentRows as any[]) {
      const fid = row.fid;
      const currentDbPoints = Number(row.currentPoints);
      const s2Points = Number(row.s2Points);

      // Get S1 points from old database (0 if user doesn't exist there)
      const oldS1Data = oldS1Map.get(fid);
      const oldS1Points = oldS1Data?.points || 0;

      // New total = Old S1 + S2 calculated
      const newTotalPoints = oldS1Points + s2Points;
      const difference = newTotalPoints - currentDbPoints;

      updates.push({
        fid,
        username: row.username || `user_${fid}`,
        currentDbPoints,
        oldS1Points,
        s2CalculatedPoints: s2Points,
        newTotalPoints,
        difference,
      });

      if (oldS1Points === 0 && s2Points > 0) {
        onlyS2Users++;
      }
    }

    // Summary statistics
    const usersWithChanges = updates.filter(u => u.difference !== 0);
    const usersGainingPoints = updates.filter(u => u.difference > 0);
    const usersLosingPoints = updates.filter(u => u.difference < 0);
    const noChange = updates.filter(u => u.difference === 0);

    const totalCurrentPoints = updates.reduce((sum, u) => sum + u.currentDbPoints, 0);
    const totalNewPoints = updates.reduce((sum, u) => sum + u.newTotalPoints, 0);

    console.log('\n' + '='.repeat(60));
    console.log('SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total users:              ${updates.length}`);
    console.log(`Users with S2 only:       ${onlyS2Users}`);
    console.log(`Users with no change:     ${noChange.length}`);
    console.log(`Users gaining points:     ${usersGainingPoints.length}`);
    console.log(`Users losing points:      ${usersLosingPoints.length}`);
    console.log('');
    console.log(`Total current points:     ${totalCurrentPoints.toLocaleString()}`);
    console.log(`Total new points:         ${totalNewPoints.toLocaleString()}`);
    console.log(`Net change:               ${(totalNewPoints - totalCurrentPoints).toLocaleString()}`);

    // Show top changes
    if (usersWithChanges.length > 0) {
      console.log('\n' + '='.repeat(60));
      console.log('TOP 20 CHANGES (sorted by |difference|)');
      console.log('='.repeat(60));
      console.log('FID       | Username         | Current | Old S1 | S2 Calc | New    | Diff');
      console.log('-'.repeat(80));

      const sorted = [...usersWithChanges].sort((a, b) => Math.abs(b.difference) - Math.abs(a.difference));
      for (const u of sorted.slice(0, 20)) {
        const diffStr = u.difference > 0 ? `+${u.difference}` : `${u.difference}`;
        console.log(
          `${u.fid.toString().padEnd(9)} | ${u.username.slice(0, 16).padEnd(16)} | ${u.currentDbPoints.toString().padStart(7)} | ${u.oldS1Points.toString().padStart(6)} | ${u.s2CalculatedPoints.toString().padStart(7)} | ${u.newTotalPoints.toString().padStart(6)} | ${diffStr}`
        );
      }
    }

    // Apply changes if not dry run
    if (!isDryRun && usersWithChanges.length > 0) {
      console.log('\n📝 Applying changes...');

      let updated = 0;
      const BATCH_SIZE = 100;

      for (let i = 0; i < updates.length; i += BATCH_SIZE) {
        const batch = updates.slice(i, i + BATCH_SIZE);

        for (const u of batch) {
          if (u.difference !== 0) {
            await currentDb.execute(
              'UPDATE users SET points = ? WHERE fid = ?',
              [u.newTotalPoints, u.fid]
            );
            updated++;
          }
        }

        process.stdout.write(`\r   Updated ${updated} users...`);
      }

      console.log(`\n✅ Updated ${updated} users`);
    } else if (isDryRun) {
      console.log('\n⚠️ DRY RUN - No changes made');
      console.log('   Run without --dry-run to apply changes');
    }

    console.log('\n' + '='.repeat(60));

  } finally {
    await currentDb.end();
    await oldDb.end();
  }
}

/**
 * Deep analysis of S1 differences
 */
async function analyzeS1Differences(currentDb: mysql.Connection, oldDb: mysql.Connection) {
  console.log('\n📊 ANALYZING S1 DIFFERENCES...\n');

  // Get sample users with biggest S1 differences
  const [oldS1Rows] = await oldDb.execute(`
    SELECT fid, points FROM users WHERE fid IS NOT NULL ORDER BY points DESC LIMIT 50
  `);
  const topOldUsers = oldS1Rows as any[];
  const fids = topOldUsers.map(u => u.fid).join(',');

  // Get their S1 vote breakdown from current DB
  const [currentS1] = await currentDb.execute(`
    SELECT
      u.fid,
      COUNT(*) as s1Votes,
      SUM(CASE WHEN v.shared = 1 THEN 1 ELSE 0 END) as sharedVotes,
      SUM(CASE WHEN v.shared = 0 THEN 1 ELSE 0 END) as notSharedVotes,
      SUM(v.pointsEarned) as calcPoints
    FROM users u
    JOIN user_brand_votes v ON v.userId = u.id
    WHERE v.season = 1 AND u.fid IN (${fids})
    GROUP BY u.fid
  `);

  const currentMap = new Map<number, any>();
  for (const row of currentS1 as any[]) {
    currentMap.set(row.fid, row);
  }

  console.log('FID       | Old S1 Pts | Votes | Shared | Calc Pts | Diff   | Pts/Vote (Old) | Pts/Vote (Calc)');
  console.log('-'.repeat(100));

  for (const oldUser of topOldUsers.slice(0, 20)) {
    const current = currentMap.get(oldUser.fid);
    if (!current) continue;

    const oldPts = Number(oldUser.points);
    const calcPts = Number(current.calcPoints);
    const votes = Number(current.s1Votes);
    const shared = Number(current.sharedVotes);
    const diff = oldPts - calcPts;
    const oldPtsPerVote = (oldPts / votes).toFixed(2);
    const calcPtsPerVote = (calcPts / votes).toFixed(2);

    console.log(
      `${oldUser.fid.toString().padEnd(9)} | ${oldPts.toString().padStart(10)} | ${votes.toString().padStart(5)} | ${shared.toString().padStart(6)} | ${calcPts.toString().padStart(8)} | ${diff.toString().padStart(6)} | ${oldPtsPerVote.padStart(14)} | ${calcPtsPerVote.padStart(15)}`
    );
  }

  console.log('\n' + '='.repeat(60));
  console.log('INSIGHT:');
  console.log('Old S1 system gave ~6 pts/vote regardless of sharing status.');
  console.log('Our backfill gave 6 pts for shared, 3 pts for not shared.');
  console.log('The difference comes from votes marked as not shared getting 3 instead of 6.');
  console.log('='.repeat(60));
}

main().catch(console.error);
