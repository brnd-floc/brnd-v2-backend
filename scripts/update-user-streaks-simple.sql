-- ============================================================================
-- Update User Streaks SQL Script (Simplified & More Compatible)
-- ============================================================================
-- Run this script step by step in DBeaver
-- ============================================================================

-- ============================================================================
-- STEP 0: DIAGNOSTIC - Check your schema first!
-- ============================================================================
-- Run this to find the correct foreign key column name:
SELECT 
    COLUMN_NAME,
    TABLE_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'user_brand_votes'
AND COLUMN_NAME LIKE '%user%'
ORDER BY COLUMN_NAME;

-- Also check if you have votes:
SELECT COUNT(*) as total_votes, COUNT(DISTINCT userId) as users_with_votes
FROM user_brand_votes;

-- ============================================================================
-- STEP 1: Create temporary table with user vote dates
-- ============================================================================
-- IMPORTANT: Replace 'userId' with the correct column name from Step 0!
-- Common options: 'userId', 'user_id', 'userFid'

DROP TEMPORARY TABLE IF EXISTS user_vote_dates;

CREATE TEMPORARY TABLE user_vote_dates AS
SELECT DISTINCT
    u.id AS user_id,
    DATE(ubv.date) AS vote_date_utc  -- Simplified: assumes dates are already in UTC or server timezone
FROM users u
INNER JOIN user_brand_votes ubv ON ubv.userId = u.id  -- CHANGE 'userId' HERE IF NEEDED
WHERE ubv.date IS NOT NULL;

-- Check if it worked:
SELECT COUNT(*) as total_vote_dates, COUNT(DISTINCT user_id) as unique_users
FROM user_vote_dates;

-- ============================================================================
-- STEP 2: Calculate MAX STREAK (simplified approach)
-- ============================================================================

DROP TEMPORARY TABLE IF EXISTS user_max_streaks;

CREATE TEMPORARY TABLE user_max_streaks AS
SELECT 
    user_id,
    MAX(streak_length) AS max_streak
FROM (
    SELECT 
        user_id,
        streak_group,
        COUNT(*) AS streak_length
    FROM (
        SELECT 
            user_id,
            vote_date_utc,
            DATE_SUB(vote_date_utc, INTERVAL @row_number:=@row_number+1 DAY) AS streak_group
        FROM user_vote_dates,
        (SELECT @row_number:=0) AS r
        ORDER BY user_id, vote_date_utc
    ) AS ranked
    GROUP BY user_id, streak_group
) AS streaks
GROUP BY user_id;

-- Check results:
SELECT * FROM user_max_streaks ORDER BY max_streak DESC LIMIT 10;

-- ============================================================================
-- STEP 3: Calculate CURRENT STREAK (simplified approach)
-- ============================================================================

DROP TEMPORARY TABLE IF EXISTS user_current_streaks;

CREATE TEMPORARY TABLE user_current_streaks AS
SELECT 
    user_id,
    CASE 
        WHEN MAX(CASE WHEN vote_date_utc = CURDATE() THEN 1 ELSE 0 END) = 1 THEN
            -- User voted today, count consecutive days backwards
            COALESCE(
                (SELECT MIN(days_ago) 
                 FROM (
                     SELECT 
                         user_id,
                         days_ago,
                         days_ago - LAG(days_ago) OVER (PARTITION BY user_id ORDER BY days_ago) AS gap
                     FROM (
                         SELECT 
                             uvd.user_id,
                             DATEDIFF(CURDATE(), uvd.vote_date_utc) AS days_ago
                         FROM user_vote_dates uvd
                         WHERE uvd.vote_date_utc <= CURDATE()
                     ) AS offsets
                 ) AS gaps
                 WHERE gap > 1 AND user_id = uvd_main.user_id),
                (SELECT MAX(DATEDIFF(CURDATE(), vote_date_utc)) + 1 
                 FROM user_vote_dates 
                 WHERE user_id = uvd_main.user_id)
            )
        ELSE 0
    END AS current_streak
FROM user_vote_dates uvd_main
GROUP BY user_id;

-- If the above doesn't work, use this simpler version:
DROP TEMPORARY TABLE IF EXISTS user_current_streaks;

CREATE TEMPORARY TABLE user_current_streaks AS
SELECT 
    uvd.user_id,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM user_vote_dates uvd2 
            WHERE uvd2.user_id = uvd.user_id 
            AND uvd2.vote_date_utc = CURDATE()
        ) THEN
            -- User voted today, find first gap going backwards
            COALESCE(
                (SELECT MIN(days_ago) 
                 FROM (
                     SELECT 
                         DATEDIFF(CURDATE(), vote_date_utc) AS days_ago,
                         DATEDIFF(CURDATE(), vote_date_utc) - 
                         (SELECT DATEDIFF(CURDATE(), vote_date_utc) 
                          FROM user_vote_dates uvd3 
                          WHERE uvd3.user_id = uvd.user_id 
                          AND uvd3.vote_date_utc < uvd2.vote_date_utc 
                          ORDER BY uvd3.vote_date_utc DESC LIMIT 1) AS gap
                     FROM user_vote_dates uvd2
                     WHERE uvd2.user_id = uvd.user_id
                     AND uvd2.vote_date_utc <= CURDATE()
                 ) AS gaps
                 WHERE gap > 1),
                (SELECT MAX(DATEDIFF(CURDATE(), vote_date_utc)) + 1 
                 FROM user_vote_dates 
                 WHERE user_id = uvd.user_id)
            )
        ELSE 0
    END AS current_streak
FROM user_vote_dates uvd
GROUP BY uvd.user_id;

-- Check results:
SELECT * FROM user_current_streaks ORDER BY current_streak DESC LIMIT 10;

-- ============================================================================
-- STEP 4: Update users table with max streaks
-- ============================================================================

UPDATE users u
INNER JOIN user_max_streaks ums ON u.id = ums.user_id
SET u.maxDailyStreak = ums.max_streak;

-- Check how many were updated:
SELECT COUNT(*) as users_updated_with_max_streak
FROM users u
INNER JOIN user_max_streaks ums ON u.id = ums.user_id;

-- ============================================================================
-- STEP 5: Update users table with current streaks
-- ============================================================================

UPDATE users u
LEFT JOIN user_current_streaks ucs ON u.id = ucs.user_id
SET u.dailyStreak = COALESCE(ucs.current_streak, 0);

-- Check how many were updated:
SELECT 
    COUNT(*) as total_users,
    SUM(CASE WHEN dailyStreak > 0 THEN 1 ELSE 0 END) as users_with_streak
FROM users;

-- ============================================================================
-- STEP 6: Clean up
-- ============================================================================

DROP TEMPORARY TABLE IF EXISTS user_vote_dates;
DROP TEMPORARY TABLE IF EXISTS user_max_streaks;
DROP TEMPORARY TABLE IF EXISTS user_current_streaks;

-- ============================================================================
-- VERIFICATION: Check results
-- ============================================================================

SELECT 
    u.id,
    u.fid,
    u.username,
    u.dailyStreak,
    u.maxDailyStreak,
    COUNT(DISTINCT DATE(ubv.date)) AS total_vote_days
FROM users u
LEFT JOIN user_brand_votes ubv ON ubv.userId = u.id  -- CHANGE 'userId' HERE IF NEEDED
GROUP BY u.id, u.fid, u.username, u.dailyStreak, u.maxDailyStreak
ORDER BY u.maxDailyStreak DESC
LIMIT 20;
