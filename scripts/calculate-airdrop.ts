/**
 * Post-Migration Script: Calculate Airdrop Scores
 *
 * This script calculates airdrop scores for the top 1111 users after database migration.
 * It calls the existing airdrop service endpoints via HTTP to avoid Bun/NestJS compatibility issues.
 */

async function calculateAirdropScores() {
  console.log(
    '# ==============================================================================',
  );
  console.log('Post-Migration: Calculating Airdrop Scores');
  console.log(
    '# ==============================================================================',
  );
  console.log('');
  console.log(
    'This will calculate airdrop scores for the top 1111 users based on:',
  );
  console.log('  • Base points from voting and activity');
  console.log(
    '  • Multipliers (follow accounts, channel interaction, BRND holdings, etc.)',
  );
  console.log('  • Final token allocation percentages');
  console.log('');

  // Check if server is running on the expected port
  const baseUrl = process.env.APP_URL || 'http://localhost:3000';

  try {
    console.log('🔍 Checking if NestJS server is accessible...');
    console.log(`  Attempting to connect to: ${baseUrl}`);

    // Test server connectivity with a simple health check
    try {
      const healthResponse = await fetch(`${baseUrl}/`, {
        method: 'GET',
      });
      console.log(`✓ Server is accessible (status: ${healthResponse.status})`);
    } catch (connectError) {
      console.error('❌ Cannot connect to NestJS server');
      console.error('');
      console.error('Please ensure your server is running with:');
      console.error('  bun start        # or');
      console.error('  bun start:dev    # for development');
      console.error('');
      console.error(`Expected server at: ${baseUrl}`);
      console.error(
        'Set APP_URL environment variable if using a different URL',
      );
      process.exit(1);
    }

    console.log('');
    console.log('📊 Starting airdrop calculation for top 1111 users...');
    console.log(
      '  This may take a few minutes depending on the number of users',
    );
    console.log('');

    // Calculate airdrop scores for all top users via HTTP endpoint
    const batchSize = 50;
    console.log(
      `  Calling: GET ${baseUrl}/airdrop-service/calculate-all-users?batchSize=${batchSize}`,
    );

    const calculateResponse = await fetch(
      `${baseUrl}/airdrop-service/calculate-all-users?batchSize=${batchSize}`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      },
    );

    if (!calculateResponse.ok) {
      throw new Error(
        `Airdrop calculation failed: ${calculateResponse.status} ${calculateResponse.statusText}`,
      );
    }

    const result = await calculateResponse.json();

    console.log('✓ Airdrop calculation completed successfully');
    console.log('');
    console.log('📈 Results:');
    console.log(`  • Users processed: ${result.usersProcessed || 'N/A'}`);
    console.log(`  • Users in top 1111: ${result.topUsersCount || 'N/A'}`);
    console.log(
      `  • Total token allocation: ${result.totalTokens?.toLocaleString() || 'N/A'}`,
    );
    console.log('');

    // Get final leaderboard to verify
    console.log('🏆 Fetching final leaderboard (top 10)...');
    console.log(
      `  Calling: GET ${baseUrl}/airdrop-service/leaderboard?limit=10`,
    );

    const leaderboardResponse = await fetch(
      `${baseUrl}/airdrop-service/leaderboard?limit=10`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      },
    );

    if (!leaderboardResponse.ok) {
      console.log('  ⚠️  Could not fetch leaderboard for verification');
    } else {
      const leaderboard = await leaderboardResponse.json();

      if (leaderboard.length > 0) {
        console.log('  Top 10 users:');
        leaderboard.forEach((user: any, index: number) => {
          const percentage =
            leaderboard[0].finalScore > 0
              ? ((user.finalScore / leaderboard[0].finalScore) * 100).toFixed(1)
              : '0.0';
          console.log(
            `    ${index + 1}. FID ${user.fid} - Score: ${user.finalScore.toLocaleString()} (${percentage}%)`,
          );
        });
      } else {
        console.log('  ⚠️  No users found in leaderboard');
      }
    }

    console.log('');
    console.log(
      '# ==============================================================================',
    );
    console.log('✓ Airdrop calculation completed successfully!');
    console.log(
      '# ==============================================================================',
    );
    console.log('');
    console.log(
      'The top 1111 users now have calculated airdrop scores and token allocations.',
    );
    console.log(
      'The daily cron job will keep these scores updated going forward.',
    );
    console.log('');
  } catch (error: any) {
    console.error('');
    console.error('❌ ERROR: Airdrop calculation failed:');
    console.error(`   ${error.message}`);

    if (error.cause) {
      console.error(`   Cause: ${error.cause.message}`);
    }

    console.error('');
    console.error('Please check:');
    console.error('  • NestJS server is running and accessible');
    console.error('  • Database connection is working');
    console.error('  • All required environment variables are set');
    console.error('  • The migration completed successfully');
    console.error('  • Server has access to required APIs (Neynar, etc.)');

    process.exit(1);
  }
}

// Run the airdrop calculation
calculateAirdropScores()
  .then(() => {
    console.log('✓ Airdrop calculation script completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Airdrop calculation script failed:');
    console.error(error);
    process.exit(1);
  });
