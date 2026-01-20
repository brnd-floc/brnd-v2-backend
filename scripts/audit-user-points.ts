#!/usr/bin/env bun

/**
 * Audit User Points Script
 *
 * Compares each user's `points` in the users table against
 * the sum of `pointsEarned` from their votes.
 *
 * Usage:
 *   bun run scripts/audit-user-points.ts
 *   bun run scripts/audit-user-points.ts --show-all    # Show all users, not just mismatches
 *   bun run scripts/audit-user-points.ts --top 50      # Show top 50 mismatches
 */

import * as mysql from 'mysql2/promise';

interface UserAudit {
  fid: number;
  username: string;
  dbPoints: number;
  calculatedPoints: number;
  difference: number;
  voteCount: number;
}

async function main() {
  const args = process.argv.slice(2);
  const showAll = args.includes('--show-all');
  const topIndex = args.indexOf('--top');
  const topN = topIndex !== -1 ? parseInt(args[topIndex + 1], 10) || 20 : 20;

  console.log('\n' + '='.repeat(60));
  console.log('         USER POINTS AUDIT');
  console.log('='.repeat(60));

  // Connect to MySQL
  const conn = await mysql.createConnection({
    host: process.env.DATABASE_HOST,
    port: parseInt(process.env.DATABASE_PORT || '3306', 10),
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_NAME,
  });

  console.log('✅ Connected to MySQL\n');

  // Get sum of pointsEarned per user vs their actual points
  const [rows] = await conn.execute(`
    SELECT
      u.fid,
      u.username,
      u.points as dbPoints,
      COALESCE(SUM(v.pointsEarned), 0) as calculatedPoints,
      COUNT(v.transactionHash) as voteCount
    FROM users u
    LEFT JOIN user_brand_votes v ON v.userId = u.id
    GROUP BY u.id, u.fid, u.username, u.points
    ORDER BY u.points DESC
  `);

  const users = (rows as any[]).map((row): UserAudit => ({
    fid: row.fid,
    username: row.username || `user_${row.fid}`,
    dbPoints: Number(row.dbPoints),
    calculatedPoints: Number(row.calculatedPoints),
    difference: Number(row.dbPoints) - Number(row.calculatedPoints),
    voteCount: Number(row.voteCount),
  }));

  // Analyze
  const mismatches = users.filter(u => u.difference !== 0);
  const overCredited = mismatches.filter(u => u.difference > 0);
  const underCredited = mismatches.filter(u => u.difference < 0);
  const matching = users.filter(u => u.difference === 0);

  const totalDbPoints = users.reduce((sum, u) => sum + u.dbPoints, 0);
  const totalCalculated = users.reduce((sum, u) => sum + u.calculatedPoints, 0);

  // Summary
  console.log('📊 SUMMARY');
  console.log('-'.repeat(60));
  console.log(`Total users:              ${users.length}`);
  console.log(`Users with matching pts:  ${matching.length}`);
  console.log(`Users with mismatches:    ${mismatches.length}`);
  console.log(`  - Over-credited:        ${overCredited.length} (DB has MORE than calculated)`);
  console.log(`  - Under-credited:       ${underCredited.length} (DB has LESS than calculated)`);
  console.log('');
  console.log(`Total DB points:          ${totalDbPoints.toLocaleString()}`);
  console.log(`Total calculated points:  ${totalCalculated.toLocaleString()}`);
  console.log(`Global difference:        ${(totalDbPoints - totalCalculated).toLocaleString()}`);

  // Show mismatches
  if (mismatches.length > 0) {
    console.log('\n' + '='.repeat(60));
    console.log(`TOP ${Math.min(topN, mismatches.length)} MISMATCHES (sorted by |difference|)`);
    console.log('='.repeat(60));
    console.log('FID       | Username         | DB Pts  | Calc Pts | Diff    | Votes');
    console.log('-'.repeat(60));

    const sorted = [...mismatches].sort((a, b) => Math.abs(b.difference) - Math.abs(a.difference));
    const toShow = showAll ? sorted : sorted.slice(0, topN);

    for (const u of toShow) {
      const diffStr = u.difference > 0 ? `+${u.difference}` : `${u.difference}`;
      console.log(
        `${u.fid.toString().padEnd(9)} | ${u.username.slice(0, 16).padEnd(16)} | ${u.dbPoints.toString().padStart(7)} | ${u.calculatedPoints.toString().padStart(8)} | ${diffStr.padStart(7)} | ${u.voteCount}`
      );
    }

    if (!showAll && sorted.length > topN) {
      console.log(`... and ${sorted.length - topN} more mismatches`);
    }

    // Breakdown by difference type
    console.log('\n' + '='.repeat(60));
    console.log('DIFFERENCE DISTRIBUTION');
    console.log('='.repeat(60));

    const buckets = {
      'Exact match (0)': matching.length,
      'Small (+1 to +10)': mismatches.filter(u => u.difference > 0 && u.difference <= 10).length,
      'Medium (+11 to +50)': mismatches.filter(u => u.difference > 10 && u.difference <= 50).length,
      'Large (+51 to +200)': mismatches.filter(u => u.difference > 50 && u.difference <= 200).length,
      'Very large (+201+)': mismatches.filter(u => u.difference > 200).length,
      'Small (-1 to -10)': mismatches.filter(u => u.difference < 0 && u.difference >= -10).length,
      'Medium (-11 to -50)': mismatches.filter(u => u.difference < -10 && u.difference >= -50).length,
      'Large (-51 to -200)': mismatches.filter(u => u.difference < -50 && u.difference >= -200).length,
      'Very large (-201-)': mismatches.filter(u => u.difference < -200).length,
    };

    for (const [label, count] of Object.entries(buckets)) {
      if (count > 0) {
        console.log(`  ${label.padEnd(25)} ${count}`);
      }
    }
  } else {
    console.log('\n✅ All users have matching points!');
  }

  console.log('\n' + '='.repeat(60));
  await conn.end();
}

main().catch(console.error);
