-- ============================================================================
-- COMPLETE SCRIPT: Update User Streaks
-- ============================================================================
-- This script calculates and updates maxDailyStreak and dailyStreak for all users
-- Run this entire script in DBeaver
-- ============================================================================

-- Clean up any existing temp tables
DROP TEMPORARY TABLE IF EXISTS user_vote_dates;
DROP TEMPORARY TABLE IF EXISTS user_max_streaks;
DROP TEMPORARY TABLE IF EXISTS user_current_streaks;

-- ============================================================================
-- STEP 1: Create table with distinct vote dates per user (UTC dates)
-- ============================================================================
-- NOTE: If you get an error on 'userId', check your schema and replace it
-- Common column names: 'userId', 'user_id', 'userFid'

CREATE TEMPORARY TABLE user_vote_dates AS
SELECT DISTINCT
    u.id AS user_id,
    DATE(ubv.date) AS vote_date
FROM users u
INNER JOIN user_brand_votes ubv ON ubv.userId = u.id
WHERE ubv.date IS NOT NULL
ORDER BY u.id, DATE(ubv.date);

-- ============================================================================
-- STEP 2: Calculate MAX STREAK for each user
-- ============================================================================

CREATE TEMPORARY TABLE user_max_streaks AS
SELECT 
    user_id,
    MAX(streak_count) AS max_streak
FROM (
    SELECT 
        user_id,
        MIN(vote_date) AS streak_start,
        MAX(vote_date) AS streak_end,
        COUNT(*) AS streak_count,
        DATEDIFF(MAX(vote_date), MIN(vote_date)) + 1 AS expected_days
    FROM (
        SELECT 
            user_id,
            vote_date,
            @prev_date := LAG(vote_date) OVER (PARTITION BY user_id ORDER BY vote_date) AS prev_date,
            @streak_group := CASE 
                WHEN @prev_date IS NULL THEN @streak_num := 1
                WHEN DATEDIFF(vote_date, @prev_date) = 1 THEN @streak_num
                ELSE @streak_num := @streak_num + 1
            END AS streak_group
        FROM user_vote_dates
        ORDER BY user_id, vote_date
    ) AS grouped
    GROUP BY user_id, streak_group
    HAVING streak_count = expected_days  -- Only count consecutive streaks
) AS streaks
GROUP BY user_id;

-- Alternative simpler max streak calculation (if above doesn't work):
-- Uncomment this and comment out the above if you get errors

/*
CREATE TEMPORARY TABLE user_max_streaks AS
SELECT 
    uvd1.user_id,
    MAX(
        (SELECT COUNT(*) 
         FROM user_vote_dates uvd2 
         WHERE uvd2.user_id = uvd1.user_id
         AND uvd2.vote_date BETWEEN uvd1.vote_date AND DATE_ADD(uvd1.vote_date, INTERVAL 30 DAY)
         AND NOT EXISTS (
             SELECT 1 
             FROM user_vote_dates uvd3 
             WHERE uvd3.user_id = uvd1.user_id
             AND uvd3.vote_date BETWEEN uvd1.vote_date AND DATE_ADD(uvd1.vote_date, INTERVAL 30 DAY)
             AND DATEDIFF(uvd3.vote_date, uvd1.vote_date) NOT IN (
                 SELECT DATEDIFF(uvd4.vote_date, uvd1.vote_date)
                 FROM user_vote_dates uvd4
                 WHERE uvd4.user_id = uvd1.user_id
                 AND uvd4.vote_date >= uvd1.vote_date
                 AND uvd4.vote_date <= DATE_ADD(uvd1.vote_date, INTERVAL 30 DAY)
                 ORDER BY uvd4.vote_date
                 LIMIT 1
             )
         )
        )
    ) AS max_streak
FROM user_vote_dates uvd1
GROUP BY uvd1.user_id;
*/

-- ============================================================================
-- STEP 3: Calculate CURRENT STREAK (consecutive days from today backwards)
-- ============================================================================

CREATE TEMPORARY TABLE user_current_streaks AS
SELECT 
    user_id,
    CASE 
        -- Check if user voted today
        WHEN MAX(CASE WHEN vote_date = CURDATE() THEN 1 ELSE 0 END) = 1 THEN
            -- User voted today, count consecutive days backwards
            (SELECT COUNT(*) 
             FROM (
                 SELECT 
                     vote_date,
                     DATEDIFF(CURDATE(), vote_date) AS days_ago,
                     @prev_days_ago := LAG(DATEDIFF(CURDATE(), vote_date)) OVER (ORDER BY vote_date DESC) AS prev_days_ago
                 FROM user_vote_dates uvd2
                 WHERE uvd2.user_id = uvd_main.user_id
                 AND uvd2.vote_date <= CURDATE()
                 ORDER BY vote_date DESC
             ) AS ordered
             WHERE days_ago = 0 OR (prev_days_ago IS NOT NULL AND days_ago = prev_days_ago - 1)
             LIMIT 100  -- Safety limit
            )
        ELSE 0
    END AS current_streak
FROM user_vote_dates uvd_main
GROUP BY user_id;

-- Simpler alternative for current streak (if above doesn't work):
-- Uncomment this and comment out the above if you get errors

/*
CREATE TEMPORARY TABLE user_current_streaks AS
SELECT 
    uvd.user_id,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM user_vote_dates uvd2 
            WHERE uvd2.user_id = uvd.user_id 
            AND uvd2.vote_date = CURDATE()
        ) THEN
            -- User voted today, find how many consecutive days going back
            (SELECT COUNT(*) 
             FROM (
                 SELECT 
                     vote_date,
                     DATEDIFF(CURDATE(), vote_date) AS days_ago
                 FROM user_vote_dates uvd3
                 WHERE uvd3.user_id = uvd.user_id
                 AND uvd3.vote_date <= CURDATE()
                 ORDER BY vote_date DESC
             ) AS ordered
             WHERE days_ago <= (
                 SELECT MIN(gap_start)
                 FROM (
                     SELECT 
                         DATEDIFF(CURDATE(), vote_date) AS days_ago,
                         DATEDIFF(CURDATE(), vote_date) - 
                         COALESCE((
                             SELECT DATEDIFF(CURDATE(), vote_date)
                             FROM user_vote_dates uvd4
                             WHERE uvd4.user_id = uvd.user_id
                             AND uvd4.vote_date < uvd3.vote_date
                             ORDER BY uvd4.vote_date DESC
                             LIMIT 1
                         ), -1) AS gap
                     FROM user_vote_dates uvd3
                     WHERE uvd3.user_id = uvd.user_id
                     AND uvd3.vote_date <= CURDATE()
                     ORDER BY vote_date DESC
                 ) AS gaps
                 WHERE gap > 1
             )
            )
        ELSE 0
    END AS current_streak
FROM user_vote_dates uvd
GROUP BY uvd.user_id;
*/

-- ============================================================================
-- STEP 4: Update maxDailyStreak in users table
-- ============================================================================

UPDATE users u
INNER JOIN user_max_streaks ums ON u.id = ums.user_id
SET u.maxDailyStreak = ums.max_streak;

-- ============================================================================
-- STEP 5: Update dailyStreak in users table
-- ============================================================================

UPDATE users u
LEFT JOIN user_current_streaks ucs ON u.id = ucs.user_id
SET u.dailyStreak = COALESCE(ucs.current_streak, 0);

-- ============================================================================
-- STEP 6: Clean up temporary tables
-- ============================================================================

DROP TEMPORARY TABLE IF EXISTS user_vote_dates;
DROP TEMPORARY TABLE IF EXISTS user_max_streaks;
DROP TEMPORARY TABLE IF EXISTS user_current_streaks;

-- ============================================================================
-- VERIFICATION: Check the results
-- ============================================================================

SELECT 
    u.id,
    u.fid,
    u.username,
    u.dailyStreak,
    u.maxDailyStreak,
    COUNT(DISTINCT DATE(ubv.date)) AS total_vote_days
FROM users u
LEFT JOIN user_brand_votes ubv ON ubv.userId = u.id
GROUP BY u.id, u.fid, u.username, u.dailyStreak, u.maxDailyStreak
ORDER BY u.maxDailyStreak DESC, u.dailyStreak DESC
LIMIT 50;
