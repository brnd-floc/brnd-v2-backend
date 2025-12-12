-- ============================================================================
-- SIMPLE COMPLETE SCRIPT: Update User Streaks
-- ============================================================================
-- This uses a simple approach that works in all MySQL/MariaDB versions
-- Run this entire script in DBeaver
-- ============================================================================

-- Clean up
DROP TEMPORARY TABLE IF EXISTS user_vote_dates;
DROP TEMPORARY TABLE IF EXISTS user_max_streaks;
DROP TEMPORARY TABLE IF EXISTS user_current_streaks;

-- ============================================================================
-- STEP 1: Get distinct vote dates per user
-- ============================================================================

CREATE TEMPORARY TABLE user_vote_dates AS
SELECT DISTINCT
    u.id AS user_id,
    DATE(ubv.date) AS vote_date
FROM users u
INNER JOIN user_brand_votes ubv ON ubv.userId = u.id  -- CHANGE 'userId' IF NEEDED
WHERE ubv.date IS NOT NULL
ORDER BY u.id, DATE(ubv.date);

-- ============================================================================
-- STEP 2: Calculate MAX STREAK using simple approach
-- ============================================================================

CREATE TEMPORARY TABLE user_max_streaks AS
SELECT 
    uvd1.user_id,
    COALESCE(MAX(streak_len), 0) AS max_streak
FROM user_vote_dates uvd1
LEFT JOIN (
    SELECT 
        uvd_a.user_id,
        uvd_a.vote_date AS streak_start,
        COUNT(*) AS streak_len
    FROM user_vote_dates uvd_a
    INNER JOIN user_vote_dates uvd_b ON uvd_a.user_id = uvd_b.user_id
    WHERE uvd_b.vote_date >= uvd_a.vote_date
    AND uvd_b.vote_date <= (
        SELECT DATE_ADD(uvd_a.vote_date, INTERVAL (COUNT(*) - 1) DAY)
        FROM user_vote_dates uvd_c
        WHERE uvd_c.user_id = uvd_a.user_id
        AND uvd_c.vote_date >= uvd_a.vote_date
        AND NOT EXISTS (
            SELECT 1
            FROM user_vote_dates uvd_d
            WHERE uvd_d.user_id = uvd_a.user_id
            AND uvd_d.vote_date > uvd_a.vote_date
            AND uvd_d.vote_date < (
                SELECT MIN(uvd_e.vote_date)
                FROM user_vote_dates uvd_e
                WHERE uvd_e.user_id = uvd_a.user_id
                AND uvd_e.vote_date > uvd_a.vote_date
                AND DATEDIFF(uvd_e.vote_date, uvd_a.vote_date) > 1
            )
        )
        GROUP BY uvd_a.user_id, uvd_a.vote_date
    )
    GROUP BY uvd_a.user_id, uvd_a.vote_date
    HAVING streak_len = DATEDIFF(MAX(uvd_b.vote_date), uvd_a.vote_date) + 1
) AS streaks ON uvd1.user_id = streaks.user_id
GROUP BY uvd1.user_id;

-- ============================================================================
-- STEP 3: Calculate CURRENT STREAK (simplified)
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
            -- Count consecutive days from today backwards
            (SELECT COUNT(*)
             FROM (
                 SELECT 
                     vote_date,
                     DATEDIFF(CURDATE(), vote_date) AS days_ago
                 FROM user_vote_dates uvd3
                 WHERE uvd3.user_id = uvd.user_id
                 AND uvd3.vote_date <= CURDATE()
                 ORDER BY vote_date DESC
             ) AS ordered_dates
             WHERE days_ago <= (
                 SELECT COALESCE(MIN(days_ago), 999)
                 FROM (
                     SELECT 
                         DATEDIFF(CURDATE(), vote_date) AS days_ago,
                         DATEDIFF(CURDATE(), vote_date) - COALESCE((
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
-- VERIFICATION
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
