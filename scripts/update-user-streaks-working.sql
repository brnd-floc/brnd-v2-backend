-- ============================================================================
-- WORKING SCRIPT: Update User Streaks
-- ============================================================================
-- Simple approach that should work in all MySQL versions
-- IMPORTANT: Change 'userId' on line 18 if your column name is different
-- ============================================================================

-- Clean up
DROP TEMPORARY TABLE IF EXISTS user_vote_dates;
DROP TEMPORARY TABLE IF EXISTS user_max_streaks;
DROP TEMPORARY TABLE IF EXISTS user_current_streaks;

-- ============================================================================
-- STEP 1: Get all distinct vote dates per user
-- ============================================================================

CREATE TEMPORARY TABLE user_vote_dates AS
SELECT DISTINCT
    u.id AS user_id,
    DATE(ubv.date) AS vote_date
FROM users u
INNER JOIN user_brand_votes ubv ON ubv.userId = u.id  -- ⚠️ CHANGE THIS IF NEEDED
WHERE ubv.date IS NOT NULL
ORDER BY u.id, DATE(ubv.date);

-- ============================================================================
-- STEP 2: Calculate MAX STREAK
-- ============================================================================
-- For each user, find the longest sequence of consecutive dates

CREATE TEMPORARY TABLE user_max_streaks AS
SELECT 
    uvd.user_id,
    COALESCE(MAX(streak_length), 0) AS max_streak
FROM user_vote_dates uvd
LEFT JOIN (
    SELECT 
        uvd1.user_id,
        uvd1.vote_date AS start_date,
        COUNT(*) AS streak_length
    FROM user_vote_dates uvd1
    INNER JOIN user_vote_dates uvd2 ON uvd1.user_id = uvd2.user_id
    WHERE uvd2.vote_date >= uvd1.vote_date
    AND uvd2.vote_date <= (
        SELECT DATE_ADD(uvd1.vote_date, INTERVAL (COUNT(*) - 1) DAY)
        FROM user_vote_dates uvd3
        WHERE uvd3.user_id = uvd1.user_id
        AND uvd3.vote_date >= uvd1.vote_date
        AND NOT EXISTS (
            SELECT 1
            FROM user_vote_dates uvd4
            WHERE uvd4.user_id = uvd1.user_id
            AND uvd4.vote_date > uvd1.vote_date
            AND uvd4.vote_date < uvd3.vote_date
            AND DATEDIFF(uvd4.vote_date, uvd1.vote_date) != (
                SELECT COUNT(*)
                FROM user_vote_dates uvd5
                WHERE uvd5.user_id = uvd1.user_id
                AND uvd5.vote_date >= uvd1.vote_date
                AND uvd5.vote_date < uvd4.vote_date
            )
        )
        LIMIT 1
    )
    GROUP BY uvd1.user_id, uvd1.vote_date
    HAVING streak_length = DATEDIFF(MAX(uvd2.vote_date), uvd1.vote_date) + 1
) AS streaks ON uvd.user_id = streaks.user_id
GROUP BY uvd.user_id;

-- ============================================================================
-- STEP 3: Calculate CURRENT STREAK (from today backwards)
-- ============================================================================

CREATE TEMPORARY TABLE user_current_streaks AS
SELECT 
    uvd.user_id,
    CASE 
        -- Check if user voted today
        WHEN EXISTS (
            SELECT 1 FROM user_vote_dates uvd2 
            WHERE uvd2.user_id = uvd.user_id 
            AND uvd2.vote_date = CURDATE()
        ) THEN
            -- Count how many consecutive days from today going backwards
            (SELECT COUNT(*)
             FROM user_vote_dates uvd3
             WHERE uvd3.user_id = uvd.user_id
             AND uvd3.vote_date <= CURDATE()
             AND uvd3.vote_date >= (
                 SELECT DATE_SUB(CURDATE(), INTERVAL (
                     SELECT COALESCE(MIN(days_ago), 365)
                     FROM (
                         SELECT 
                             DATEDIFF(CURDATE(), vote_date) AS days_ago
                         FROM user_vote_dates uvd4
                         WHERE uvd4.user_id = uvd.user_id
                         AND uvd4.vote_date <= CURDATE()
                         AND NOT EXISTS (
                             SELECT 1
                             FROM user_vote_dates uvd5
                             WHERE uvd5.user_id = uvd.user_id
                             AND uvd5.vote_date = DATE_SUB(uvd4.vote_date, INTERVAL 1 DAY)
                         )
                         AND uvd4.vote_date < CURDATE()
                     ) AS gaps
                 ) DAY)
             )
            )
        ELSE 0
    END AS current_streak
FROM user_vote_dates uvd
GROUP BY uvd.user_id;

-- ============================================================================
-- STEP 4: Update users table
-- ============================================================================

UPDATE users u
LEFT JOIN user_max_streaks ums ON u.id = ums.user_id
SET u.maxDailyStreak = COALESCE(ums.max_streak, 0);

UPDATE users u
LEFT JOIN user_current_streaks ucs ON u.id = ucs.user_id
SET u.dailyStreak = COALESCE(ucs.current_streak, 0);

-- ============================================================================
-- STEP 5: Clean up
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
LEFT JOIN user_brand_votes ubv ON ubv.userId = u.id
GROUP BY u.id, u.fid, u.username, u.dailyStreak, u.maxDailyStreak
ORDER BY u.maxDailyStreak DESC, u.dailyStreak DESC
LIMIT 50;
