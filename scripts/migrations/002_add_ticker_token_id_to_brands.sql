-- Migration: Add tickerTokenId to brands (idempotent)
-- Description: Stores optional CAIP-19 token identifier for multi-chain ticker metadata
-- Date: 2026-02-17

SET @column_exists := (
  SELECT COUNT(*)
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'brands'
    AND COLUMN_NAME = 'tickerTokenId'
);

SET @ddl := IF(
  @column_exists = 0,
  'ALTER TABLE `brands` ADD COLUMN `tickerTokenId` varchar(120) NULL AFTER `contractAddress`',
  'SELECT ''Column tickerTokenId already exists'' AS info'
);

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
