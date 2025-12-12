-- ============================================================================
-- COMPLETE SCRIPT: Update User Streaks (Simple & Compatible)
-- ============================================================================
-- Run this entire script in DBeaver
-- If you get an error on 'userId', replace it with your actual column name
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
-- STEP 2: Calculate MAX STREAK (longest consecutive sequence)
-- ============================================================================

CREATE TEMPORARY TABLE user_max_streaks AS
SELECT 
    uvd1.user_id,
    COALESCE(MAX(streak_length), 0) AS max_streak
FROM user_vote_dates uvd1
LEFT JOIN (
    SELECT 
        uvd_a.user_id,
        uvd_a.vote_date AS start_date,
        COUNT(*) AS streak_length
    FROM user_vote_dates uvd_a
    INNER JOIN user_vote_dates uvd_b ON uvd_a.user_id = uvd_b.user_id
    WHERE uvd_b.vote_date >= uvd_a.vote_date
    AND uvd_b.vote_date <= DATE_ADD(uvd_a.vote_date, INTERVAL (
        SELECT COUNT(*) - 1
        FROM user_vote_dates uvd_c
        WHERE uvd_c.user_id = uvd_a.user_id
        AND uvd_c.vote_date >= uvd_a.vote_date
        AND uvd_c.vote_date <= DATE_ADD(uvd_a.vote_date, INTERVAL 365 DAY)
        AND NOT EXISTS (
            SELECT 1
            FROM user_vote_dates uvd_d
            WHERE uvd_d.user_id = uvd_a.user_id
            AND uvd_d.vote_date = DATE_ADD(uvd_a.vote_date, INTERVAL (
                SELECT COUNT(*)
                FROM user_vote_dates uvd_e
                WHERE uvd_e.user_id = uvd_a.user_id
                AND uvd_e.vote_date >= uvd_a.vote_date
                AND uvd_e.vote_date < uvd_d.vote_date
            ) DAY)
        )
    ) DAY)
    GROUP BY uvd_a.user_id, uvd_a.vote_date
    HAVING streak_length = DATEDIFF(MAX(uvd_b.vote_date), uvd_a.vote_date) + 1
) AS streaks ON uvd1.user_id = streaks.user_id
GROUP BY uvd1.user_id;

-- Simpler max streak (if above is too complex):
DROP TEMPORARY TABLE IF EXISTS user_max_streaks;

CREATE TEMPORARY TABLE user_max_streaks AS
SELECT 
    user_id,
    MAX(consecutive_days) AS max_streak
FROM (
    SELECT 
        uvd1.user_id,
        uvd1.vote_date AS start_date,
        COUNT(*) AS consecutive_days
    FROM user_vote_dates uvd1
    INNER JOIN user_vote_dates uvd2 ON uvd1.user_id = uvd2.user_id
    WHERE uvd2.vote_date >= uvd1.vote_date
    AND uvd2.vote_date <= DATE_ADD(uvd1.vote_date, INTERVAL (
        SELECT COUNT(*) - 1
        FROM user_vote_dates uvd3
        WHERE uvd3.user_id = uvd1.user_id
        AND uvd3.vote_date >= uvd1.vote_date
        AND uvd3.vote_date <= DATE_ADD(uvd1.vote_date, INTERVAL 100 DAY)
        AND NOT EXISTS (
            SELECT 1
            FROM user_vote_dates uvd4
            WHERE uvd4.user_id = uvd1.user_id
            AND uvd4.vote_date > uvd1.vote_date
            AND uvd4.vote_date < uvd3.vote_date
            AND uvd4.vote_date NOT IN (
                SELECT DATE_ADD(uvd1.vote_date, INTERVAL n DAY)
                FROM (
                    SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION
                    SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION
                    SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION
                    SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION
                    SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION
                    SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION
                    SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION
                    SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION
                    SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION
                    SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION
                    SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION
                    SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION
                    SELECT 60 UNION SELECT 61 UNION SELECT 62 UNION SELECT 63 UNION SELECT 64 UNION
                    SELECT 65 UNION SELECT 66 UNION SELECT 67 UNION SELECT 68 UNION SELECT 69 UNION
                    SELECT 70 UNION SELECT 71 UNION SELECT 72 UNION SELECT 73 UNION SELECT 74 UNION
                    SELECT 75 UNION SELECT 76 UNION SELECT 77 UNION SELECT 78 UNION SELECT 79 UNION
                    SELECT 80 UNION SELECT 81 UNION SELECT 82 UNION SELECT 83 UNION SELECT 84 UNION
                    SELECT 85 UNION SELECT 86 UNION SELECT 87 UNION SELECT 88 UNION SELECT 89 UNION
                    SELECT 90 UNION SELECT 91 UNION SELECT 92 UNION SELECT 93 UNION SELECT 94 UNION
                    SELECT 95 UNION SELECT 96 UNION SELECT 97 UNION SELECT 98 UNION SELECT 99
                ) AS numbers
                WHERE DATE_ADD(uvd1.vote_date, INTERVAL n DAY) <= uvd3.vote_date
            )
        )
    ) DAY)
    GROUP BY uvd1.user_id, uvd1.vote_date
    HAVING consecutive_days = DATEDIFF(MAX(uvd2.vote_date), uvd1.vote_date) + 1
) AS all_streaks
GROUP BY user_id;

-- ============================================================================
-- STEP 3: Calculate CURRENT STREAK (from today backwards)
-- ============================================================================

CREATE TEMPORARY TABLE user_current_streaks AS
SELECT 
    user_id,
    CASE 
        WHEN MAX(CASE WHEN vote_date = CURDATE() THEN 1 ELSE 0 END) = 1 THEN
            -- User voted today, count backwards
            (SELECT COUNT(*)
             FROM user_vote_dates uvd2
             WHERE uvd2.user_id = uvd_main.user_id
             AND uvd2.vote_date <= CURDATE()
             AND uvd2.vote_date >= DATE_SUB(CURDATE(), INTERVAL (
                 SELECT COALESCE(MIN(gap_day), 365)
                 FROM (
                     SELECT 
                         DATEDIFF(CURDATE(), vote_date) AS days_ago,
                         DATEDIFF(CURDATE(), vote_date) - COALESCE((
                             SELECT DATEDIFF(CURDATE(), vote_date)
                             FROM user_vote_dates uvd3
                             WHERE uvd3.user_id = uvd_main.user_id
                             AND uvd3.vote_date < uvd2.vote_date
                             ORDER BY uvd3.vote_date DESC
                             LIMIT 1
                         ), -1) AS gap,
                         DATEDIFF(CURDATE(), vote_date) AS gap_day
                     FROM user_vote_dates uvd2
                     WHERE uvd2.user_id = uvd_main.user_id
                     AND uvd2.vote_date <= CURDATE()
                     ORDER BY vote_date DESC
                 ) AS gaps
                 WHERE gap > 1
             ) DAY)
            )
        ELSE 0
    END AS current_streak
FROM user_vote_dates uvd_main
GROUP BY user_id;

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
