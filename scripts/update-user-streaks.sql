-- ============================================================================
-- Update User Streaks SQL Script
-- ============================================================================
-- This script calculates and updates:
-- 1. maxDailyStreak: The longest consecutive streak of voting days ever
-- 2. dailyStreak: The current consecutive streak from today going backwards
--
-- Notes:
-- - Day ends at midnight UTC
-- - Multiple votes on the same day count as one day
-- - Uses the date column (timestamp) converted to UTC date
-- - Counts ALL votes (not just those with transactionHash)
--
-- IMPORTANT: Before running, verify the foreign key column name!
-- Check your database schema - it might be 'userId', 'user_id', or 'userFid'
-- Update line 21 accordingly: `ubv.userId` or `ubv.user_id` or `ubv.userFid`
-- ============================================================================

-- Step 1: Create a temporary table with user vote dates (UTC dates only)
-- Note: Adjust 'userId' if your foreign key column has a different name
CREATE TEMPORARY TABLE IF NOT EXISTS user_vote_dates AS
SELECT DISTINCT
    u.id AS user_id,
    DATE(CONVERT_TZ(ubv.date, @@session.time_zone, '+00:00')) AS vote_date_utc
FROM users u
INNER JOIN user_brand_votes ubv ON ubv.userId = u.id
WHERE ubv.date IS NOT NULL;

-- Step 2: Calculate MAX STREAK for each user
-- This finds the longest consecutive sequence of voting days
CREATE TEMPORARY TABLE IF NOT EXISTS user_max_streaks AS
WITH ranked_dates AS (
    SELECT 
        user_id,
        vote_date_utc,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY vote_date_utc) AS rn
    FROM user_vote_dates
),
streak_groups AS (
    SELECT 
        user_id,
        vote_date_utc,
        DATE_SUB(vote_date_utc, INTERVAL rn DAY) AS streak_group
    FROM ranked_dates
),
streak_lengths AS (
    SELECT 
        user_id,
        streak_group,
        COUNT(*) AS streak_length
    FROM streak_groups
    GROUP BY user_id, streak_group
)
SELECT 
    user_id,
    MAX(streak_length) AS max_streak
FROM streak_lengths
GROUP BY user_id;

-- Step 3: Calculate CURRENT STREAK for each user
-- This counts consecutive days backwards from today (UTC)
CREATE TEMPORARY TABLE IF NOT EXISTS user_current_streaks AS
WITH today_utc AS (
    SELECT DATE(CONVERT_TZ(NOW(), @@session.time_zone, '+00:00')) AS today
),
-- Get vote dates with days_ago calculation (0 = today, 1 = yesterday, etc.)
user_dates_with_offset AS (
    SELECT 
        uvd.user_id,
        uvd.vote_date_utc,
        DATEDIFF(t.today, uvd.vote_date_utc) AS days_ago
    FROM user_vote_dates uvd
    CROSS JOIN today_utc t
    WHERE uvd.vote_date_utc <= t.today  -- Only dates from today or earlier
),
-- For each user, find where the streak breaks (first gap going backwards from today)
-- A gap occurs when days_ago jumps by more than 1
streak_analysis AS (
    SELECT 
        user_id,
        days_ago,
        LAG(days_ago) OVER (PARTITION BY user_id ORDER BY days_ago) AS prev_days_ago,
        -- Mark the missing day where gap occurs (prev_days_ago + 1)
        CASE 
            -- If first vote is not today (days_ago > 0), gap starts at 0 (today)
            WHEN LAG(days_ago) OVER (PARTITION BY user_id ORDER BY days_ago) IS NULL 
                AND days_ago > 0 THEN 0
            -- If gap detected (difference > 1), gap is at prev_days_ago + 1
            WHEN days_ago - LAG(days_ago) OVER (PARTITION BY user_id ORDER BY days_ago) > 1 
                THEN LAG(days_ago) OVER (PARTITION BY user_id ORDER BY days_ago) + 1
            ELSE NULL
        END AS gap_at
    FROM user_dates_with_offset
),
-- Find the first gap for each user (closest to today)
first_gap AS (
    SELECT 
        user_id,
        MIN(gap_at) AS first_gap_day
    FROM streak_analysis
    WHERE gap_at IS NOT NULL
    GROUP BY user_id
),
-- Calculate current streak for each user
current_streak_calc AS (
    SELECT 
        udwo.user_id,
        -- If user voted today (days_ago = 0), count consecutive days to first gap
        -- If no gap, count all their recent votes
        CASE 
            -- User voted today - count from 0 to first gap (or all if no gap)
            WHEN EXISTS (
                SELECT 1 FROM user_dates_with_offset ud2 
                WHERE ud2.user_id = udwo.user_id AND ud2.days_ago = 0
            ) THEN
                COALESCE(fg.first_gap_day, MAX(udwo.days_ago) + 1)
            -- User didn't vote today - streak is 0
            ELSE 0
        END AS current_streak
    FROM user_dates_with_offset udwo
    LEFT JOIN first_gap fg ON udwo.user_id = fg.user_id
    GROUP BY udwo.user_id, fg.first_gap_day
)
SELECT 
    user_id,
    MAX(current_streak) AS current_streak
FROM current_streak_calc
GROUP BY user_id;

-- Step 4: Update users table with max streaks
UPDATE users u
INNER JOIN user_max_streaks ums ON u.id = ums.user_id
SET u.maxDailyStreak = ums.max_streak;

-- Step 5: Update users table with current streaks
UPDATE users u
LEFT JOIN user_current_streaks ucs ON u.id = ucs.user_id
SET u.dailyStreak = COALESCE(ucs.current_streak, 0);

-- Step 6: Clean up - drop temporary tables
DROP TEMPORARY TABLE IF EXISTS user_vote_dates;
DROP TEMPORARY TABLE IF EXISTS user_max_streaks;
DROP TEMPORARY TABLE IF EXISTS user_current_streaks;

-- ============================================================================
-- Verification Query (optional - run this to check results)
-- ============================================================================
-- SELECT 
--     u.id,
--     u.fid,
--     u.username,
--     u.dailyStreak,
--     u.maxDailyStreak,
--     COUNT(DISTINCT DATE(CONVERT_TZ(ubv.date, @@session.time_zone, '+00:00'))) AS total_vote_days
-- FROM users u
-- LEFT JOIN user_brand_votes ubv ON ubv.userId = u.id
-- GROUP BY u.id, u.fid, u.username, u.dailyStreak, u.maxDailyStreak
-- ORDER BY u.maxDailyStreak DESC
-- LIMIT 20;
-- ============================================================================
